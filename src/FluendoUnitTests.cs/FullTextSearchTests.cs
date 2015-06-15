using System;
using Couchbase.Lite;
using System.Collections.Generic;
using NUnit.Framework;

namespace FluendoUnitTests
{
    public class FullTextSearchTests
    {
        [Test ()]
        public void FullTextSearchTest()
        {
            Console.WriteLine("Starting performance test!");

            Manager manager = new Manager(new System.IO.DirectoryInfo("/home/andoni/"),
                ManagerOptions.Default);
            Database db = manager.GetDatabase("test");
            db.MaxRevTreeDepth = 1;

            DateTime time = DateTime.UtcNow;
            db.Compact();
            Console.WriteLine(string.Format("Compacted db in {0}",
                (DateTime.Now - time).ToString(@"s\.fff")));
            time = DateTime.Now;

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
            Console.WriteLine(string.Format("Stored {0} elements in {1}",
                c, (DateTime.Now - time).ToString(@"s\.fff")));
            time = DateTime.Now;

            View view = db.GetView("Events");
            if (view.Map == null) {
                view.SetMap((document, emitter) =>
                {
                    {
                        List<string> keys = new List<string> {
                            document["player"].ToString(), document["team"].ToString(),
                            document["event_type"].ToString(), document["event_name"].ToString()
                        };
                        FullTextKey k1 = new FullTextKey(String.Join(" ", keys));
                        emitter(k1, document);
                        emitter(keys, document);
                    }
                }, "1");
            }
            Console.WriteLine(string.Format("Added view in {0}",
                (DateTime.Now - time).ToString(@"s\.fff")));
            time = DateTime.Now;

            Query q = view.CreateQuery();

            Console.WriteLine(string.Format("Created query in {0}",
                (DateTime.Now - time).ToString(@"s\.fff")));
            time = DateTime.Now;


            q.FullTextSearch = "andoni";
            QueryEnumerator ret = q.Run();
            Console.WriteLine(string.Format("Performed query in {0}",
                (DateTime.Now - time).ToString(@"s\.fff")));
            Assert.AreEqual (120, ret.Count);
            time = DateTime.Now;


            q.FullTextSearch = "andoni falta madrid";
            ret = q.Run();
            Console.WriteLine(string.Format("Performed query in {0}",
                (DateTime.Now - time).ToString(@"s\.fff")));
            Assert.AreEqual (10, ret.Count);
            time = DateTime.Now;

            q.FullTextSearch = "jorge barça";
            ret = q.Run();
            Console.WriteLine(string.Format("Performed query in {0}",
                (DateTime.Now - time).ToString(@"s\.fff")));
            Assert.AreEqual (40, ret.Count);
            time = DateTime.Now;

            db.RunInTransaction(() =>
            {
                for (int i = 0; i < c; i++) {
                    db.GetExistingDocument(i.ToString()).Delete();
                }
                return true;
            });
            Console.WriteLine(string.Format("Deleted {0} documents in {1}",
                c, (DateTime.Now - time).ToString(@"s\.fff")));
        }

    }
}

