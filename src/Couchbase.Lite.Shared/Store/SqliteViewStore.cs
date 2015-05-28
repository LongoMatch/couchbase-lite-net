﻿//
//  SqliteViewStore.cs
//
//  Author:
//  	Jim Borden  <jim.borden@couchbase.com>
//
//  Copyright (c) 2015 Couchbase, Inc All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
using System;
using Couchbase.Lite.Util;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Couchbase.Lite.Internal;

namespace Couchbase.Lite.Store
{
    internal sealed class SqliteViewStore : IViewStore, IQueryRowStore
    {

        #region Constants

        private const string TAG = "SqliteViewStore";

        #endregion

        #region Variables

        private SqliteCouchStore _dbStorage;
        private int _viewId;
        private ViewCollation _collation;

        #region Properties

        public string Name { get; private set; }

        public IViewStoreDelegate Delegate { get; set; }

        public uint TotalRows
        {
            get {
                var db = _dbStorage;
                var totalRows = db.QueryOrDefault<int>(c => c.GetInt(0), false, 0, "SELECT total_docs FROM views WHERE name=?", Name);
                if (totalRows == -1) { //means unknown
                    CreateIndex();
                    totalRows = db.QueryOrDefault<int>(c => c.GetInt(0), false, 0, QueryString("SELECT COUNT(*) FROM 'maps_#'"));
                    var args = new ContentValues();
                    args["total_docs"] = totalRows;
                    db.StorageEngine.Update("views", args, "view_id=?", ViewID);
                }

                Debug.Assert(totalRows >= 0);
                return totalRows;
            }
        }

        public long LastSequenceIndexed
        {
            get {
                return _dbStorage.QueryOrDefault<long>(c => c.GetLong(0), false, 0, "SELECT lastSequence FROM views WHERE name=?", Name);
            }
        }

        public long LastSequenceChangedAt
        {
            get {
                return LastSequenceIndexed;
                //FIXME: Should store this properly; it helps optimize CBLLiveQuery
            }
        }

        private int ViewID
        {
            get {
                if (_viewId < 0) {
                    _viewId = _dbStorage.QueryOrDefault<int>(c => c.GetInt(0), false, 0, "SELECT view_id FROM views WHERE name=?", Name);
                }

                return _viewId;
            }
        }

        private string MapTableName
        {
            get {
                if(_mapTableName == null) {
                    _mapTableName = ViewID.ToString();
                }

                return _mapTableName;
            }
        }
        private string _mapTableName;

        #endregion

        #region Constructors

        public SqliteViewStore(ICouchStore store, string name, bool create)
        {
        }

        #endregion

        #region Private Methods

        private static string ViewNames(IEnumerable<SqliteViewStore> inputViews)
        {
            throw new NotImplementedException();
        }

        private bool RunStatements(string sqlStatements)
        {
            throw new NotImplementedException();
        }

        private string QueryString(string statement)
        {
            throw new NotImplementedException();
        }

        private void CreateIndex()
        {
            throw new NotImplementedException();
        }


        private StatusCode Emit(object key, object value, bool valueIsDoc, long sequence)
        {
            throw new NotImplementedException();
        }
            
        private void FinishCreatingIndex()
        {
            throw new NotImplementedException();
        }

        //TODO: bbox
        private bool CreateRTreeSchema()
        {
            throw new NotImplementedException();
        }

        private bool GroupTogether(IEnumerable<byte> keyData, IEnumerable<byte> lastKeyData, int groupLevel)
        {
            throw new NotImplementedException();
        }

        private object GroupKey(IEnumerable<byte> lastKeyData, int groupLevel)
        {
            throw new NotImplementedException();
        }

        private object CallReduce(ReduceDelegate reduce, List<object> keysToReduce, List<object> valuesToReduce)
        {
            throw new NotImplementedException();
        }

        private Status RunQuery(QueryOptions options, Func<IEnumerable<byte>, IEnumerable<byte>, string, Cursor, Status> action)
        {
            if (options == null) {
                options = new QueryOptions();
            }

            string collationStr = "";
            if (_collation == ViewCollation.ASCII) {
                collationStr = " COLLATE JSON_ASCII ";
            } else if (_collation == ViewCollation.Raw) {
                collationStr = " COLLATE JSON_RAW ";
            }

            var sql = new StringBuilder("SELECT key, value, docid, revs.sequence");
            if (options.IncludeDocs) {
                sql.Append(", revid, json");
            }

            if (false) {
                //TODO: bbox
                if (!CreateRTreeSchema()) {
                    return new Status(StatusCode.NotImplemented);
                }

                sql.AppendFormat(", bboxes.x0, bboxes.y0, bboxes.x1, bboxes.y1, maps_{0}.geokey", MapTableName);
            }

            sql.AppendFormat(" FROM 'maps_{0}', revs, docs", MapTableName);
            if (false) {
                //TODO: bbox
                sql.Append(", bboxes");
            }

            sql.Append(" WHERE 1 ");
            var args = new List<object>();
            if (options.Keys != null) {
                sql.Append(" AND key IN (");
                var item = "?";
                foreach (var key in options.Keys) {
                    sql.Append(item);
                    item = ",?";
                    args.Add(Manager.GetObjectMapper().WriteValueAsBytes(key));
                }
                sql.Append(")");
            }

            var minKey = options.StartKey;
            var maxKey = options.EndKey;
            var minKeyDocId = options.StartKeyDocId;
            var maxKeyDocId = options.EndKeyDocId;
            bool inclusiveMin = options.InclusiveStart;
            bool inclusiveMax = options.InclusiveEnd;
            if (options.Descending) {
                minKey = options.EndKey;
                maxKey = options.StartKey;
                inclusiveMin = options.InclusiveEnd;
                inclusiveMax = options.InclusiveStart;
                minKeyDocId = options.EndKeyDocId;
                maxKeyDocId = options.StartKeyDocId;
            }

            if (minKey != null) {
                var minKeyData = Manager.GetObjectMapper().WriteValueAsBytes(minKey);
                sql.Append(inclusiveMin ? " AND key >= ?" : " AND key > ?");
                sql.Append(collationStr);
                args.Add(minKeyData);
                if (minKeyDocId != null && inclusiveMin) {
                    sql.AppendFormat(" AND (key > ? {0} OR docid >= ?)", collationStr);
                    args.Add(minKeyData);
                    args.Add(minKeyDocId);
                }
            }

            if (maxKey != null) {
                maxKey = KeyForPrefixMatch(maxKey, options.PrefixMatchLevel);
                var maxKeyData = Manager.GetObjectMapper().WriteValueAsBytes(maxKey);
                sql.Append(inclusiveMax ? " AND key <= ?" : " AND key < ?");
                sql.Append(collationStr);
                args.Add(maxKeyData);
                if (maxKeyDocId != null && inclusiveMax) {
                    sql.AppendFormat(" AND (key < ? {0} OR docid <= ?)", collationStr);
                    args.Add(maxKeyData);
                    args.Add(maxKeyDocId);
                }
            }

            if (false) {
                //TODO: bbox
                sql.AppendFormat(" AND (bboxes.x1 > ? AND bboxes.x0 < ?)" +
                    " AND (bboxes.y1 > ? AND bboxes.y0 < ?)" +
                    " AND bboxes.rowid = 'maps_{0}'.bbox_id", MapTableName);
                
            }

            sql.AppendFormat(" AND revs.sequence = 'maps_{0}'.sequence AND docs.doc_id = revs.doc_id " +
                "ORDER BY", MapTableName);
            if (false) {
                //TODO: bbox
                sql.Append(" bboxes.y0, bboxes.x0");
            } else {
                sql.Append(" key");
            }

            sql.Append(collationStr);
            if (options.Descending) {
                sql.Append(" DESC");
            }

            sql.Append(options.Descending ? ", docid DESC" : ", docid");
            sql.Append("LIMIT ? OFFSET ?");
            int limit = options.Limit != QueryOptions.DEFAULT_LIMIT ? options.Limit : -1;
            args.Add(limit);
            args.Add(options.Skip);

            Log.D(TAG, "Query {0}: {1}\n\tArguments: {2}", Name, sql, Manager.GetObjectMapper().WriteValueAsString(args));

            var dbStorage = _dbStorage;
            var status = new Status();
            dbStorage.TryQuery(c => 
            {
                var keyData = c.GetBlob(0);
                var docId = c.GetString(2);
                Debug.Assert(keyData != null);

                // Call the block!
                var valueData = c.GetBlob(1);
                status = action(keyData, valueData, docId, c);
                if(status.IsError) {
                    return false;
                } else if((int)status.Code <= 0) {
                    status.Code = StatusCode.Ok;
                    return true;
                }
            }, false, sql, args);

            return status;
        }

        #endregion

        #region IViewStore

        public void Close()
        {
            _dbStorage = null;
            _viewId = -1;
        }

        public void DeleteIndex()
        {
            if (ViewID <= 0) {
                return;
            }

            const string sql = "DROP TABLE IF EXISTS 'maps_#';UPDATE views SET lastSequence=0, total_docs=0 WHERE view_id=#";
            if (!RunStatements(sql)) {
                Log.W(TAG, "Couldn't delete view index `{0}`", Name);
            }
        }

        public void DeleteView()
        {
            var db = _dbStorage;
            db.RunInTransaction(() =>
            {
                DeleteIndex();
                try {
                    db.StorageEngine.Delete("views", "name=?", Name);
                } catch(Exception e) {
                    return new Status(StatusCode.DbError);
                }

                return new Status(StatusCode.Ok);
            });

            _viewId = 0;
        }

        public bool SetVersion(string version)
        {
            // Update the version column in the db. This is a little weird looking because we want to
            // avoid modifying the db if the version didn't change, and because the row might not exist yet.
            var db = _dbStorage;
            var args = new ContentValues();
            args["name"] = Name;
            args["version"] = version;
            args["total_docs"] = 0;

            long changes = 0;
            try {
                db.StorageEngine.InsertWithOnConflict("views", null, args, ConflictResolutionStrategy.Ignore);
            } catch(Exception) {
                return false;
            }

            if (changes > 0) {
                CreateIndex();
                return true; //created new view
            }

            try {
                args = new ContentValues();
                args["version"] = version;
                args["lastSequence"] = 0;
                args["total_docs"] = 0;
                db.StorageEngine.Update("views", args, "name=? AND version!=?", Name, version);
            } catch(Exception) {
                return false;
            }

            return true;
        }

        public Status UpdateIndexes(IEnumerable<SqliteViewStore> inputViews)
        {
            Log.D(TAG, "Checking indexes of ({0}) for {1}", ViewNames(inputViews), Name);
            var db = _dbStorage;

            var status = db.RunInTransaction(() =>
            {
                // If the view the update is for doesn't need any update, don't do anything:
                long dbMaxSequence = db.LastSequence;
                long forViewLastSequence = LastSequenceIndexed;
                if(forViewLastSequence >= dbMaxSequence) {
                    return new Status(StatusCode.NotModified);
                }

                // Check whether we need to update at all,
                // and remove obsolete emitted results from the 'maps' table:
                long minLastSequence = db.LastSequence;
                long[] viewLastSequence = new long[inputViews.Count()];
                int deletedCount = 0;
                int i = 0;
                HashSet<string> docTypes = new HashSet<string>();
                IDictionary<string, string> viewDocTypes = null;
                bool allDocTypes = false;
                IDictionary<int, int> viewTotalRows = new Dictionary<int, int>();
                List<SqliteViewStore> views = new List<SqliteViewStore>(inputViews.Count());
                List<MapDelegate> mapBlocks = new List<MapDelegate>();
                foreach(var view in inputViews) {
                    var viewDelegate = view.Delegate;
                    var mapBlock = viewDelegate == null ? null : viewDelegate.MapBlock;
                    if(mapBlock == null) {
                        Debug.Assert(view != this, String.Format("Cannot index view {0}: no map block registered", view.Name));
                        Log.V(TAG, "    {0} has no map block; skipping it", view.Name);
                        continue;
                    }

                    views.Add(view);
                    mapBlocks.Add(mapBlock);

                    int viewId = view.ViewID;
                    Debug.Assert(viewId > 0, String.Format("View '{0}' not found in database", view.Name));

                    int totalRows = view.TotalRows;
                    viewTotalRows[viewId] = totalRows;

                    long last = view == this ? forViewLastSequence : view.LastSequenceIndexed;
                    viewLastSequence[i++] = last;
                    if(last < 0) {
                        return new Status(StatusCode.DbError);
                    }

                    if(last < dbMaxSequence) {
                        if(last == 0) {
                            CreateIndex();
                        }

                        minLastSequence = Math.Min(minLastSequence, last);
                        Log.V(TAG, "    {0} last indexed at #{1}", view.Name, last);

                        string docType = viewDelegate.DocumentType;
                        if(docType != null) {
                            docTypes.Add(docType);
                            if(viewDocTypes == null) {
                                viewDocTypes = new Dictionary<string, string>();
                            }

                            viewDocTypes[view.Name] = docType;
                        } else {
                            // can't filter by doc_type
                            allDocTypes = true; 
                        }

                        bool ok;
                        int changes = 0;
                        if(last == 0) {
                            try {
                                // If the lastSequence has been reset to 0, make sure to remove all map results:
                                changes = db.StorageEngine.ExecSQL(view.QueryString("DELETE FROM 'maps_#'"));
                            } catch(Exception) {
                                ok = false;
                            }
                        } else {
                            db.OptimizeSQLIndexes(); // ensures query will use the right indexes
                            // Delete all obsolete map results (ones from since-replaced revisions):
                            try {
                                changes = db.StorageEngine.ExecSQL(view.QueryString("DELETE FROM 'maps_#' WHERE sequence IN (" +
                                    "SELECT parent FROM revs WHERE sequence>? " +
                                    "AND +parent>0 AND +parent<=?)"), last, last);
                            } catch(Exception) {
                                ok = false;
                            }
                        }

                        if(!ok) {
                            return new Status(StatusCode.DbError);
                        }

                        // Update #deleted rows
                        deletedCount += changes;

                        // Only count these deletes as changes if this isn't a view reset to 0
                        if(last != 0) {
                            viewTotalRows[viewId] -= changes;
                        }
                    }
                }

                if(minLastSequence == dbMaxSequence) {
                    return new Status(StatusCode.NotModified);
                }

                Log.D(TAG, "Updating indexes of ({0}) from #{1} to #{2} ...",
                    ViewNames(views), minLastSequence, dbMaxSequence);

                // This is the emit() block, which gets called from within the user-defined map() block
                // that's called down below.
                SqliteViewStore currentView = null;
                IDictionary<string, object> currentDoc = null;
                long sequence = minLastSequence;
                Status emitStatus = new Status();
                int insertedCount = 0;
                EmitDelegate emit = (key, value) =>
                {
                    StatusCode s = currentView.Emit(key, value, value == currentDoc, sequence);
                    if(s != StatusCode.Ok) {
                        emitStatus.Code = s;
                    } else {
                        viewTotalRows[currentView.ViewID] += 1;
                        insertedCount++;
                    }
                };

                // Now scan every revision added since the last time the views were indexed:
                bool checkDocTypes = docTypes.Count > 1 || (allDocTypes && docTypes.Count > 0);
                var sql = new StringBuilder("SELECT revs.doc_id, sequence, docid, revid, json, deleted ");
                if(checkDocTypes) {
                    sql.Append(", doc_type ");
                }

                sql.Append("FROM revs, docs WHERE sequence>? AND current!=0 ");
                if(minLastSequence == 0) {
                    sql.Append("AND deleted=0 ");
                }

                if(!allDocTypes && docTypes.Count > 0) {
                    sql.AppendFormat("AND doc_type IN ({0}) ", Database.JoinQuoted(docTypes));
                }

                sql.Append("AND revs.doc_id = docs.doc_id " +
                    "ORDER BY revs.doc_id, deleted, revid DESC");

                Cursor c = null;
                Cursor c2 = null;
                try {
                    c = db.StorageEngine.IntransactionRawQuery(sql, minLastSequence);
                    bool keepGoing = c.MoveToNext();
                    while(keepGoing) {
                        // Get row values now, before the code below advances 'c':
                        long doc_id = c.GetLong(0);
                        sequence = c.GetLong(1);
                        string docId = c.GetString(2);
                        if(docId.StartsWith("_design/")) { // design documents don't get indexed
                            keepGoing = c.MoveToNext();
                            continue;
                        }

                        string revId = c.GetString(3);
                        var json = c.GetBlob(4);
                        bool deleted = c.GetInt(5) != 0;
                        string docType = checkDocTypes ? c.GetString(6) : null;

                        // Skip rows with the same doc_id -- these are losing conflicts.
                        while((keepGoing = c.MoveToNext()) && c.GetLong(0) == doc_id) {}

                        long realSequence = sequence; // because sequence may be changed, below
                        if(minLastSequence < 0) {
                            // Find conflicts with documents from previous indexings.
                            using(c2 = db.StorageEngine.IntransactionRawQuery("SELECT revid, sequence FROM revs " +
                                "WHERE doc_id=? AND sequence<=? AND current!=0 AND deleted=0 " +
                                "ORDER BY revID DESC " +
                                "LIMIT 1", doc_id, minLastSequence)) {

                                if(c2.MoveToNext()) {
                                    string oldRevId = c2.GetString(0);
                                    // This is the revision that used to be the 'winner'.
                                    // Remove its emitted rows:
                                    long oldSequence = c2.GetLong(1);
                                    foreach(var view in views) {
                                        int changes = db.StorageEngine.ExecSQL(QueryString("DELETE FROM 'maps_#' WHERE sequence=?"), oldSequence);
                                        deletedCount += changes;
                                        viewTotalRows[view.ViewID] -= changes;
                                    }

                                    if(deleted || RevisionInternal.CBLCompareRevIDs(oldRevId, revId) > 0) {
                                        // It still 'wins' the conflict, so it's the one that
                                        // should be mapped [again], not the current revision!
                                        revId = oldRevId;
                                        deleted = false;
                                        sequence = oldSequence;
                                        json = db.QueryOrDefault<IEnumerable<byte>>(x => x.GetBlob(0), true, null, "SELECT json FROM revs WHERE sequence=?", sequence);

                                    }
                                }
                            }
                        }

                        if(deleted) {
                            continue;
                        }

                        // Get the document properties, to pass to the map function:
                        currentDoc = db.GetDocumentProperties(json, docId, revId, deleted, sequence);
                        if(currentDoc == null) {
                            Log.W(TAG, "Failed to parse JSON of doc {0} rev {1}", docId, revId);
                            continue;
                        }

                        currentDoc["_local_seq"] = sequence;

                        // Call the user-defined map() to emit new key/value pairs from this revision:
                        int viewIndex = -1;
                        var e = views.GetEnumerator();
                        while(e.MoveNext()) {
                            currentView = e.Current;
                            ++i;
                            if(viewLastSequence[i] < realSequence) {
                                if(checkDocTypes) {
                                    var viewDocType = viewDocTypes[currentView.Name];
                                    if(viewDocType != null && viewDocType != docType) {
                                        // skip; view's documentType doesn't match this doc
                                        continue;
                                    }
                                }

                                Log.V(TAG, "    #{0}: map \"{1}\" for view {2}...",
                                    sequence, docId, curView.Name);
                                try {
                                    mapBlocks[i](currentDoc, emit);
                                } catch(Exception x) {
                                    Log.E(TAG, String.Format("Exception in map() block for view {0}", currentView.Name), x);
                                    emitStatus.Code = StatusCode.Exception;
                                }

                                if(emitStatus.IsError) {
                                    c.Dispose();
                                    return emitStatus;
                                }
                            }
                        }

                        currentView = null;
                    }
                } catch(Exception) {
                    return new Status(StatusCode.DbError);
                } finally {
                    if(c != null) {
                        c.Dispose();
                    }
                }

                // Finally, record the last revision sequence number that was indexed and update #rows:
                foreach(var view in views) {
                    view.FinishCreatingIndex();
                    int newTotalRows = viewTotalRows[view.ViewID];
                    Debug.Assert(newTotalRows >= 0);

                    var args = new ContentValues();
                    args["lastSequence"] = dbMaxSequence;
                    args["total_docs"] = newTotalRows;
                    try {
                        db.StorageEngine.Update("views", args, "view_id=?", view.ViewID);
                    } catch(Exception) {
                        return new Status(StatusCode.DbError);
                    }
                }

                Log.D(TAG, "...Finished re-indexing ({0}) to #{1} (deleted {2}, added {3})",
                    ViewNames(views), dbMaxSequence, deletedCount, insertedCount);
                return new Status(StatusCode.Ok);
            });

            if(status.Code >= StatusCode.BadRequest) {
                Log.W(TAG, "CouchbaseLite: Failed to rebuild views ({0}): {1}", ViewNames(inputViews), status);
            }

            return status;
        }

        public IEnumerable<QueryRow> RegularQuery(QueryOptions options)
        {
            var db = _dbStorage;
            var filter = options.Filter;
            int limit = int.MaxValue;
            int skip = 0;
            if (filter != null) {
                // Custom post-filter means skip/limit apply to the filtered rows, not to the
                // underlying query, so handle them specially:
                limit = options.Limit;
                skip = options.Skip;
                options.Limit = QueryOptions.DEFAULT_LIMIT;
                options.Skip = 0;
            }

            var rows = new List<QueryRow>();
            RunQuery(options, (keyData, valueData, docId, cursor) =>
            {
                long sequence = cursor.GetLong(3);
                RevisionInternal docRevision = null;
                if(options.IncludeDocs) {
                    IDictionary<string, object> value = null;
                    if(valueData != null && RowValueDataIsEntireDoc(valueData)) {
                        value = Manager.GetObjectMapper().ReadValue<IDictionary<string, object>>(valueData);
                    }

                    string linkedId = value.GetCast<string>("_id");
                    if(linkedId != null) {
                        // Linked document: http://wiki.apache.org/couchdb/Introduction_to_CouchDB_views#Linked_documents
                        string linkedRev = value.GetCast<string>("_rev"); //usually null
                        docRevision = db.GetDocument(linkedId, linkedRev, true);
                        sequence = docRevision == null ? 0 : docRevision.GetSequence();
                    } else {
                        docRevision = db.GetRevision(docId, cursor.GetString(4), false, sequence, cursor.GetBlob(5));
                    }
                }

                Log.V(TAG, "Query {0}: Found row with key={1}, value={2}, id={3}",
                    Name, BitConverter.ToString(keyData.ToArray()), BitConverter.ToString(valueData.ToArray()),
                    Manager.GetObjectMapper().WriteValueAsString(docId));

                QueryRow row = null;
                if(false) {
                    //TODO: bbox
                } else {
                    row = new QueryRow(docId, sequence, keyData, valueData, docRevision, this);
                }

                if(filter != null) {
                    if(!filter(row)) {
                        return new Status(StatusCode.Ok);
                    }

                    if(skip > 0) {
                        --skip;
                        return new Status(StatusCode.Ok);
                    }
                }

                rows.Add(row);
                if(limit-- == 0) {
                    return 0;
                }

                return new Status(StatusCode.Ok);
            });

            // If given keys, sort the output into that order, and add entries for missing keys:
            if (options.Keys != null) {
                // Group rows by key:
                var rowsByKey = new Dictionary<object, List<QueryRow>>();
                foreach (var row in rows) {
                    var dictRows = rowsByKey.Get(row.Key);
                    if (dictRows == null) {
                        dictRows = rowsByKey[row.Key] = new List<QueryRow>();
                    }

                    dictRows.Add(row);
                }

                // Now concatenate them in the order the keys are given in options:
                var sortedRows = new List<QueryRow>();
                foreach (var key in options.Keys) {
                    var dictRows = rowsByKey.Get(rows.Key);
                    if (dictRows != null) {
                        sortedRows.AddRange(dictRows);
                    }
                }

                rows = sortedRows;
            }

            return rows;
        }

        public IEnumerable<QueryRow> ReducedQuery(QueryOptions options)
        {
            var db = _dbStorage;
            var groupLevel = options.GroupLevel;
            bool group = options.Group || groupLevel > 0;
            var reduce = Delegate.ReduceBlock;
            if (options.ReduceSpecified) {
                if (options.Reduce && reduce == null) {
                    Log.W(TAG, "Cannot use reduce option in view {0} which has no reduce block defined", Name);
                    return null;
                }
            }

            List<object> keysToReduce = null, valuesToReduce = null;
            if (reduce != null) {
                keysToReduce = new List<object>(100);
                valuesToReduce = new List<object>(100);
            }

            IEnumerable<byte> lastKeyData = null;
            List<QueryRow> rows = new List<QueryRow>();
            var outStatus = RunQuery(options, (keyData, valueData, docID, c) =>
            {
                if(group && !GroupTogether(keyData, lastKeyData, groupLevel)) {
                    if(lastKeyData != null) {
                        // This pair starts a new group, so reduce & record the last one:
                        var key = GroupKey(lastKeyData, groupLevel);
                        var reduced = CallReduce(reduce, keysToReduce, valuesToReduce);
                        var row = new QueryRow(null, 0, key, reduced, null, this);
                        if(options.Filter == null || options.Filter(row)) {
                            rows.Add(row);
                        }

                        keysToReduce.Clear();
                        valuesToReduce.Clear();
                    }
                    lastKeyData = keyData;
                }

                Log.V(TAG, "    Query {0}: Will reduce row with key={1}, value={2}", Name, Encoding.UTF8.GetString(keyData),
                    Encoding.UTF8.GetString(valueData));

                object valueOrData = valueData;
                if(valuesToReduce != null && RowValueDataIsEntireDoc(valueData)) {
                    // map fn emitted 'doc' as value, which was stored as a "*" placeholder; expand now:
                    Status status = new Status();
                    var rev = db.GetDocument(docID, c.GetLong(1), status);
                    if(rev == null) {
                        Log.W(TAG, "Couldn't load doc for row value: status {0}", status.GetCode());
                    }

                    valueOrData = rev.GetProperties();
                }

                keysToReduce.Add(keyData);
                valuesToReduce.Add(valueData);
                return new Status(StatusCode.Ok);
            });

            if((keysToReduce != null && keysToReduce.Count > 0) || lastKeyData != null) {
                // Finish the last group (or the entire list, if no grouping):
                var key = group ? GroupKey(lastKeyData, groupLevel) : null;
                var reduced = CallReduce(reduce, keysToReduce, valuesToReduce);
                Log.V(TAG, "    Query {0}: Will reduce row with key={1}, value={2}", Name, Encoding.UTF8.GetString(keyData),
                    Encoding.UTF8.GetString(valueData));

                var row = new QueryRow(null, 0, key, reduced, null, this);
                if (options.filter == null || options.filter(row)) {
                    rows.Add(row);
                }
            }

            return rows;
        }

        public IQueryRowStore StorageForQueryRow(QueryRow row)
        {
            return this;
        }

        #if DEBUG

        public IEnumerable<IDictionary<string, object>> Dump()
        {
            if (ViewID <= 0) {
                return null;
            }

            List<IDictionary<string, object>> retVal = new List<IDictionary<string, object>>();
            _dbStorage.TryQuery(c =>
            {
                retVal.Add(new Dictionary<string, object>() {
                    { "seq", c.GetLong(0) },
                    { "key", c.GetString(1) },
                    { "val", c.GetString(2) }
                });

                return true;
            }, false, QueryString("SELECT sequence, key, value FROM 'maps_#' ORDER BY key"));
        }

        #endif

        #endregion

        #region IQueryRowStore

        public bool RowValueIsEntireDoc(IEnumerable<byte> valueData)
        {
            return valueData.FirstOrDefault() == (byte)'*' && valueData.Count() == 1;
        }

        public T ParseRowValue<T>(IEnumerable<byte> valueData)
        {
            return Manager.GetObjectMapper().ReadValue<T>(valueData);
        }

        public IDictionary<string, object> DocumentProperties(string docId, long sequenceNumber)
        {
            return _dbStorage.GetDocument(docId, sequenceNumber);
        }

        #endregion
    }
}

