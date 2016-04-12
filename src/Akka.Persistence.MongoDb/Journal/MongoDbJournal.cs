using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence.Journal;
using MongoDB.Driver;

namespace Akka.Persistence.MongoDb.Journal
{
    /// <summary>
    /// An Akka.NET journal implementation that writes events asynchronously to MongoDB.
    /// </summary>
    public class MongoDbJournal : AsyncWriteJournal
    {
        private readonly IMongoCollection<JournalEntry> _collection;

        public MongoDbJournal()
        {
            _collection = MongoDbPersistence.Instance.Apply(Context.System).JournalCollection;
        }

        public override Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> replayCallback)
        {
            // Limit(0) doesn't work...
            if (max == 0)
                return Task.Run(() => { });

            // Limit allows only integer
            var maxValue = max >= int.MaxValue ? int.MaxValue : (int)max;
            var sender = context.Sender;
            var builder = Builders<JournalEntry>.Filter;
            var filter = builder.Eq(x => x.PersistenceId, persistenceId);
            var sort = Builders<JournalEntry>.Sort.Ascending(x => x.SequenceNr);

            if (fromSequenceNr > 0)
                filter &= builder.Gte(x => x.SequenceNr, fromSequenceNr);

            if (toSequenceNr != long.MaxValue)
                filter &= builder.Lte(x => x.SequenceNr, toSequenceNr);

            return
                _collection
                    .Find(filter)
                    .Sort(sort)
                    .Limit(maxValue)
                    .ForEachAsync(doc => replayCallback(ToPersistanceRepresentation(doc, sender)));
        }

        public override Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            var builder = Builders<JournalEntry>.Filter;
            var filter = builder.Eq(x => x.PersistenceId, persistenceId);

            return
                _collection
                    .Find(filter)
                    .SortByDescending(x => x.SequenceNr)
                    .Limit(1)
                    .Project(x => x.SequenceNr)
                    .FirstOrDefaultAsync();
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            //@@TODO write Commits 
            var writeTasks = messages.Select(async message =>
            {
                var persistentMessages = ((IImmutableList<IPersistentRepresentation>)message.Payload).ToArray();
                var journalEntries = persistentMessages.Select(ToJournalEntry).ToList();
                await _collection.InsertManyAsync(journalEntries, new InsertManyOptions()
                {
                    IsOrdered = true
                });
            });

            return await Task<IImmutableList<Exception>>
                .Factory
                .ContinueWhenAll(writeTasks.ToArray(),
                    tasks => tasks.Select(t => t.IsFaulted ? TryUnwrapException(t.Exception) : null).ToImmutableList());

        }

        //@@TODO make protected on base class
        private Exception TryUnwrapException(Exception e)
        {
            var aggregateException = e as AggregateException;
            if (aggregateException != null)
            {
                aggregateException = aggregateException.Flatten();
                if (aggregateException.InnerExceptions.Count == 1)
                    return aggregateException.InnerExceptions[0];
            }
            return e;
        }


        //        protected override Task WriteMessagesAsync(IEnumerable<IPersistentRepresentation> messages)
        //{
        //    var entries = messages.Select(ToJournalEntry).ToList();
        //    return _collection.InsertManyAsync(entries);
        //}

        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            var builder = Builders<JournalEntry>.Filter;
            var filter = builder.Eq(x => x.PersistenceId, persistenceId);

            if (toSequenceNr != long.MaxValue)
                filter &= builder.Lte(x => x.SequenceNr, toSequenceNr);

            //@@TODO old code, commented due to isPermanent flag missing on 1.0.7
            //  I think the permanet flag && isDeleted bit should be  kept to enable SequenceNr increment on cleared stream
            //var update = Builders<JournalEntry>.Update.Set(x => x.IsDeleted, true);
            //if (isPermanent)
            //    return _collection.DeleteManyAsync(filter);
            //return _collection.UpdateManyAsync(filter, update);

            //@@TODO pass failure (exception?) to caller (check deleteManyAsync result)
            return _collection.DeleteManyAsync(filter);
        }

        private JournalEntry ToJournalEntry(IPersistentRepresentation message)
        {
            return new JournalEntry
            {
                Id = message.PersistenceId + "_" + message.SequenceNr,
                IsDeleted = message.IsDeleted,
                Payload = message.Payload,
                PersistenceId = message.PersistenceId,
                SequenceNr = message.SequenceNr,
                Manifest = message.Manifest
            };
        }

        private Persistent ToPersistanceRepresentation(JournalEntry entry, IActorRef sender)
        {
            return new Persistent(entry.Payload, entry.SequenceNr, entry.Manifest, entry.PersistenceId, entry.IsDeleted, sender);
        }
    }
}
