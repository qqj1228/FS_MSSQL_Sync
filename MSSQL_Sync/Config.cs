using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

namespace MSSQL_Sync {
    public class Config {
        public struct MainConfig {
            public int Interval { get; set; } // 单位秒
            public int NewTestLine { get; set; } // 是否用于新线
        }

        public struct DBMainConfig {
            public string SyncConfig { get; set; }
            public int MESDBStartIndex { get; set; }
            public List<string> IDColList;
        }

        public struct DBInfoConfig {
            public string IP { get; set; }
            public string Port { get; set; }
            public string UserID { get; set; }
            public string Pwd { get; set; }
            public string Name { get; set; }
            public List<int> LastIDList;
            public List<string> TableList;
        }

        public MainConfig Main;
        public DBMainConfig DB;
        public List<DBInfoConfig> DBInfoList;
        readonly Logger Log;
        string ConfigFile { get; set; }
        public Dictionary<string, string> SyncDicMES, SyncDicNL;

        public Config(Logger Log, string strConfigFile = "./config/config.xml") {
            this.Log = Log;
            this.DBInfoList = new List<DBInfoConfig>();
            this.SyncDicMES = new Dictionary<string, string>();
            this.SyncDicNL = new Dictionary<string, string>();
            this.ConfigFile = strConfigFile;
            LoadConfig();
            LoadSyncConfig();
        }

        ~Config() {
            SaveConfig();
        }

        void LoadConfig() {
            try {
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load(ConfigFile);
                XmlNode xnRoot = xmlDoc.SelectSingleNode("Config");
                XmlNodeList xnl = xnRoot.ChildNodes;

                foreach (XmlNode node in xnl) {
                    XmlNodeList xnlChildren = node.ChildNodes;
                    if (node.Name == "Main") {
                        foreach (XmlNode item in xnlChildren) {
                            if (item.Name == "Interval") {
                                int.TryParse(item.InnerText, out int result);
                                Main.Interval = result;
                            } else if (item.Name == "NewTestLine") {
                                int.TryParse(item.InnerText, out int result);
                                Main.NewTestLine = result;
                            }
                        }
                    } else if (node.Name == "DB") {
                        foreach (XmlNode item in xnlChildren) {
                            if (item.Name == "SyncConfig") {
                                DB.SyncConfig = item.InnerText;
                            } else if (item.Name == "MESDBStartIndex") {
                                int.TryParse(item.InnerText, out int result);
                                DB.MESDBStartIndex = result;
                            } else if (item.Name == "IDColList") {
                                DB.IDColList = new List<string>(item.InnerText.Split(','));
                            }
                        }
                    } else if (node.Name == "DBInfo") {
                        DBInfoConfig TempExDB = new DBInfoConfig();

                        foreach (XmlNode item in xnlChildren) {
                            XmlNodeList xnlSubChildren = item.ChildNodes;
                            foreach (XmlNode subItem in xnlSubChildren) {
                                if (subItem.Name == "IP") {
                                    TempExDB.IP = subItem.InnerText;
                                } else if (subItem.Name == "Port") {
                                    TempExDB.Port = subItem.InnerText;
                                } else if (subItem.Name == "UserID") {
                                    TempExDB.UserID = subItem.InnerText;
                                } else if (subItem.Name == "Pwd") {
                                    TempExDB.Pwd = subItem.InnerText;
                                } else if (subItem.Name == "Name") {
                                    TempExDB.Name = subItem.InnerText;
                                } else if (subItem.Name == "LastIDList") {
                                    List<string> strList = new List<string>(subItem.InnerText.Split(','));
                                    TempExDB.LastIDList = new List<int>();
                                    foreach (string str in strList) {
                                        int.TryParse(str, out int result);
                                        TempExDB.LastIDList.Add(result);
                                    }
                                } else if (subItem.Name == "TableList") {
                                    TempExDB.TableList = new List<string>(subItem.InnerText.Split(','));
                                }
                            }
                            DBInfoList.Add(TempExDB);
                        }
                    }
                }
            } catch (Exception e) {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: " + e.Message);
                Console.ResetColor();
                Log.TraceError(e.Message);
            }
        }

        public void SaveConfig() {
            try {
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load(ConfigFile);
                XmlNode xnRoot = xmlDoc.SelectSingleNode("Config");
                XmlNodeList xnl = xnRoot.ChildNodes;

                foreach (XmlNode node in xnl) {
                    XmlNodeList xnlChildren = node.ChildNodes;
                    // 只操作了需要被修改的配置项
                    if (node.Name == "DBInfo") {
                        for (int i = 0; i < DBInfoList.Count; i++) {
                            XmlNodeList xnlSubChildren = xnlChildren[i].ChildNodes;
                            foreach (XmlNode item in xnlSubChildren) {
                                if (item.Name == "LastIDList") {
                                    item.InnerText = string.Join(",", DBInfoList[i].LastIDList);
                                }
                            }
                        }
                    }
                }

                xmlDoc.Save(ConfigFile);
            } catch (Exception e) {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: " + e.Message);
                Console.ResetColor();
                Log.TraceError(e.Message);
            }
        }

        void LoadSyncConfig() {
            try {
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load("./config/" + this.DB.SyncConfig);
                XmlNode xnRoot = xmlDoc.SelectSingleNode("Sync");
                XmlNodeList xnl = xnRoot.ChildNodes;

                foreach (XmlNode node in xnl) {
                    if (node.Name != "#comment") {
                        SyncDicMES.Add(node.Name, node.InnerText);
                        if (node.InnerText != "") {
                            SyncDicNL.Add(node.InnerText, node.Name);
                        }
                    }
                }
            } catch (Exception e) {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: " + e.Message);
                Console.ResetColor();
                Log.TraceError(e.Message);
            }
        }
    }
}
