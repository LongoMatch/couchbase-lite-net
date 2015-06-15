using NUnit.Framework;
using System;
using Couchbase.Lite;
using System.Collections.Generic;

namespace FluendoUnitTests
{
    [TestFixture()]
    public class MultiKeySearch
    {
        [Test()]
        public void TestCase()
        {
            Manager manager = new Manager(new System.IO.DirectoryInfo("/tmp"),
                ManagerOptions.Default);
            Database db = manager.GetDatabase("test");
            db.MaxRevTreeDepth = 1;

            int c = 0;
            db.RunInTransaction(() =>
            {
                foreach (string player in new []{"andoni", "jorge", "julien", "josep", "xavi", "adrid", "albert"}) {
                    foreach (string team in new []{"barça", "madrid", "murcia"}) {
                        foreach (string action in new []{"gol", "falta", "penalty", "pase"}) {
                            int j = 10;
                            for (int i = 0; i < j; i++) {
                                Document doc = db.GetDocument(c.ToString());
                                doc.Update((UnsavedRevision rev) =>
                                {
                                    IDictionary<string, object> props = new Dictionary<string, object>();
                                    props["player"] = player;
                                    props["team"] = team;
                                    props["event_type"] = action;
                                    props["event_name"] = action + i;
                                    rev.SetProperties(props);
                                    return true;
                                });
                                c++;
                            }
                        }
                    }
                }
                return true;
            });

            View view = db.GetView("Events");
            if (view.Map == null) {
                view.SetMap((document, emitter) =>
                {
                    {
                        List<object> keys = new List<object> {
                            document["player"], document["team"],
                            document["event_type"], document["event_name"]
                        };
                        var mk = new PropertyKey  (keys);
                        var fk = new FullTextKey (String.Format ("{0}, {1}, {2}, {3}",
                            keys[0], keys[1], keys[2], keys[3]));
                        emitter(new MultiKey (mk, fk), document);
                    }
                }, "1");
            }
            Query q = view.CreateQuery();
            q.SQLSearch = "key='\"andoni\"' AND key1='\"madrid\"'";
            QueryEnumerator ret = q.Run();
            Assert.AreEqual (40, ret.Count);

            q.SQLSearch = "key='\"andoni\"'";
            ret = q.Run();
            Assert.AreEqual (120, ret.Count);

            q.SQLSearch = "key1='\"madrid\"'";
            ret = q.Run();
            Assert.AreEqual (280, ret.Count);

            q.SQLSearch = "key1 IN ('\"madrid\"', '\"barça\"')";
            ret = q.Run();
            Assert.AreEqual (560, ret.Count);

            q.SQLSearch = "key='\"andoni\"' AND key1 IN ('\"madrid\"', '\"barça\"')";
            ret = q.Run();
            Assert.AreEqual (80, ret.Count);

            q.FullTextSearch = "andoni madrid";
            ret = q.Run();
            Assert.AreEqual (40, ret.Count);

            q.FullTextSearch = "andoni";
            ret = q.Run();
            Assert.AreEqual (120, ret.Count);

            q.FullTextSearch = "madrid";
            ret = q.Run();
            Assert.AreEqual (280, ret.Count);

            q.FullTextSearch = "madrid gol";
            ret = q.Run();
            Assert.AreEqual (70, ret.Count);

            q.FullTextSearch = "jorge madrid gol1";
            ret = q.Run();
            Assert.AreEqual (1, ret.Count);
        }
    }
}

