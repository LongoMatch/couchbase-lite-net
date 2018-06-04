using NUnit.Framework;
using System;
using Couchbase.Lite;
using System.Collections.Generic;

namespace FluendoUnitTests
{
    [TestFixture ()]
    public class SQLSearch
    {
        Manager manager;
        Database db;

        [SetUp]
        public void SetUp ()
        {
            manager = new Manager (new System.IO.DirectoryInfo ("/tmp"),
                                                          ManagerOptions.Default);
            db = manager.GetDatabase ("test" + new Guid ().ToString ());
            db.SetMaxRevTreeDepth (0);
        }

        [Test ()]
        public void TestCase ()
        {
            int c = 0;
            db.RunInTransaction (() => {
                foreach (string player in new [] { "andoni", "jorge", "julien", "josep", "xavi", "adrid", "albert" }) {
                    foreach (string team in new [] { "barça", "madrid", "murcia" }) {
                        foreach (string action in new [] { "gol", "falta", "penalty", "pase" }) {
                            int j = 10;
                            for (int i = 0; i < j; i++) {
                                Document doc = db.GetDocument (c.ToString ());
                                doc.Update ((UnsavedRevision rev) => {
                                    IDictionary<string, object> props = new Dictionary<string, object> ();
                                    props ["player"] = player;
                                    props ["team"] = team;
                                    props ["event_type"] = action;
                                    props ["event_name"] = action + i;
                                    rev.SetProperties (props);
                                    return true;
                                });
                                c++;
                            }
                        }
                    }
                }
                return true;
            });

            View view = db.GetView ("Events");
            if (view.Map == null) {
                view.SetMap ((document, emitter) => {
                    {
                        List<object> keys = new List<object> {
                            document["player"], document["team"],
                            document["event_type"], document["event_name"]
                        };
                        var mk = new PropertyKey (keys);
                        emitter (mk, document);
                    }
                }, "1");
            }
            Query q = view.CreateQuery ();
            q.SQLSearch = "key='\"andoni\"' AND key1='\"madrid\"'";
            QueryEnumerator ret = q.Run ();
            Assert.AreEqual (40, ret.Count);

            q.SQLSearch = "key='\"andoni\"'";
            ret = q.Run ();
            Assert.AreEqual (120, ret.Count);

            q.SQLSearch = "key1='\"madrid\"'";
            ret = q.Run ();
            Assert.AreEqual (280, ret.Count);

            q = view.CreateQuery ();
            q.SQLSearch = "key1 IN ('\"madrid\"', '\"barça\"')";
            ret = q.Run ();
            Assert.AreEqual (560, ret.Count);

            q = view.CreateQuery ();
            q.SQLSearch = "key='\"andoni\"' AND key1 IN ('\"madrid\"', '\"barça\"')";
            ret = q.Run ();
            Assert.AreEqual (80, ret.Count);
        }

        [Test ()]
        public void TestDelete ()
        {

            // Arrange
            Guid c = new Guid ();
            Document doc = db.GetDocument (c.ToString ());
            doc.Update ((UnsavedRevision rev) => {
                IDictionary<string, object> props = new Dictionary<string, object> ();
                props.Add ("foo", "bar");
                rev.SetProperties (props);
                return true;
            });

            // Action
            Document storedDoc = db.GetExistingDocument (c.ToString ());
            storedDoc.Purge ();

            //db.Compact ();

            // Assert
            Document deletedDoc = db.GetExistingDocument (c.ToString ());

            Assert.IsNull (deletedDoc);
        }
    }
}