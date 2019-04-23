using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Timers;
using static MSSQL_Sync.Config;

namespace MSSQL_Sync {
    class Program {
        public struct InsertRecord {
            public string DBName { get; set; }
            public string TableName { get; set; }
            public Dictionary<string, string> Dic { get; set; }
        }

        static Logger log;
        static Config cfg;
        static Model db;

        static void Main(string[] args) {
            Console.WriteLine("Application started, input \"exit\" to close this application");
            log = new Logger("./log", EnumLogLevel.LogLevelAll, true, 100);
            cfg = new Config(log);
            db = new Model(cfg, log);
            Timer timer = new Timer(cfg.Main.Interval * 1000);
            timer.Elapsed += TimerJob;
            timer.Enabled = true;

            string line = "";
            while (line != "exit") {
                line = Console.ReadLine();
            }
            timer.Close();
            timer.Dispose();
        }

        static void TimerJob(object sender, ElapsedEventArgs e) {
            Console.WriteLine("Start to sync database");
            SyncDB();
        }

        static void SyncDB() {
            List<InsertRecord> InsertListFromNL = GetNewNLDBData();
            List<InsertRecord> InsertListFromMES = GetNewMESDBData();
            InsertNLDBToMESDB(InsertListFromNL);
            InsertMESDBToNLDB(InsertListFromMES);
            List<List<int>> LastIDContainList = new List<List<int>>();
            GetNLLastID(out LastIDContainList);
            GetMESLastID(LastIDContainList);
            UpdateLastID(LastIDContainList);
        }

        /// <summary>
        /// 获取New Line数据库中的新数据，返回一个List<InsertRecord>结构包含有可以插入MES数据库中的数据
        /// </summary>
        /// <returns></returns>
        static List<InsertRecord> GetNewNLDBData() {
            List<InsertRecord> InsertList = new List<InsertRecord>();
            // 用于存储需要更新ABS_Result的VIN号
            HashSet<string> VIN_ABS = new HashSet<string>();
            // 生成新数据记录List
            List<Dictionary<string, string>> NewList = new List<Dictionary<string, string>>();
            for (int i = 0; i < cfg.DB.MESDBStartIndex; i++) {
                int len = cfg.DBInfoList[i].TableList.Count;
                if (cfg.DBInfoList[i].LastIDList.Count == len) {
                    string DBName = cfg.DBInfoList[i].Name;
                    for (int j = 0; j < len; j++) {
                        string TableName = cfg.DBInfoList[i].TableList[j];
                        string[,] rs;
                        // 对EmissionTotal、DieselTotal表的主键做特殊处理
                        if (TableName == "EmissionTotal") {
                            rs = db.GetNewRecords(TableName, "PK_DMS_AUTO_INDEX_EmissionTotal", cfg.DBInfoList[i].LastIDList[j].ToString(), DBName);
                        } else if (TableName == "DieselTotal") {
                            rs = db.GetNewRecords(TableName, "PK_DMS_AUTO_INDEX_DieselTotal", cfg.DBInfoList[i].LastIDList[j].ToString(), DBName);
                        } else {
                            rs = db.GetNewRecords(TableName, "ID", cfg.DBInfoList[i].LastIDList[j].ToString(), DBName);
                        }
                        if (rs != null) {
                            int rowNum = rs.GetLength(0);
                            string[] colNew = db.GetTableColumns(TableName, DBName);
                            int IDIndex = 0;
                            if (rowNum > 0) {
                                for (int n = 0; n < rowNum; n++) {
                                    Dictionary<string, string> dic = new Dictionary<string, string>();
                                    for (int k = 0; k < colNew.Length; k++) {
                                        if (!cfg.DB.IDColList.Contains(colNew[k])) {
                                            dic.Add(DBName + "." + TableName + "." + colNew[k], rs[n, k]);
                                        } else {
                                            IDIndex = k;
                                        }
                                    }
                                    NewList.Add(dic);
                                }
                            }
                        }
                    }
                } else {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("ERROR: \"LastIDList\" in New Line \"DBInfo\" error");
                    Console.ResetColor();
                    log.TraceError("\"LastIDList\" in New Line \"DBInfo\" error");
                    return InsertList;
                }
            }

            // 生成需插入数据记录的List
            List<Dictionary<string, string>> InsertMESList = new List<Dictionary<string, string>>();
            foreach (Dictionary<string, string> listItem in NewList) {
                SortedDictionary<string, string> dicSortMES = new SortedDictionary<string, string>();
                foreach (KeyValuePair<string, string> dicItem in listItem) {
                    if (dicItem.Key.EndsWith(".VIN")) {
                        dicSortMES.Add("VIN", dicItem.Value);
                    }
                    if (cfg.SyncDicNL.ContainsKey(dicItem.Key) && cfg.SyncDicNL[dicItem.Key] != "") {
                        dicSortMES.Add(cfg.SyncDicNL[dicItem.Key], dicItem.Value);
                    }
                }
                if (dicSortMES.Count > 1) {
                    // 处理New对MES一对多的情况
                    // 根据key将一条Dictionary分成多条
                    Dictionary<string, string> dicSolo = new Dictionary<string, string>();
                    string keyName = "";
                    foreach (KeyValuePair<string, string> item in dicSortMES) {
                        string[] key = item.Key.Split('.');
                        if (item.Key != "VIN") {
                            if (keyName != key[0] + "." + key[1] && keyName != "") {
                                dicSolo.Add(keyName + ".VIN", dicSortMES["VIN"]);
                                InsertMESList.Add(dicSolo);
                                dicSolo = new Dictionary<string, string>();
                            }
                            dicSolo.Add(item.Key, item.Value);
                            keyName = key[0] + "." + key[1];
                        }
                    }
                    dicSolo.Add(keyName + ".VIN", dicSortMES["VIN"]);
                    InsertMESList.Add(dicSolo);
                }
            }

            // 生成InsertRecord结构List
            List<InsertRecord> RawList = new List<InsertRecord>();
            foreach (Dictionary<string, string> dic in InsertMESList) {
                InsertRecord ir = new InsertRecord {
                    DBName = "",
                    TableName = ""
                };
                Dictionary<string, string> dicInsert = new Dictionary<string, string>();
                foreach (KeyValuePair<string, string> item in dic) {
                    string[] keyArray = item.Key.Split('.');
                    if (ir.TableName == "" && ir.DBName == "") {
                        ir.DBName = keyArray[0];
                        ir.TableName = keyArray[1];
                    }
                    dicInsert.Add(keyArray[2], item.Value);
                }
                ir.Dic = dicInsert;
                RawList.Add(ir);
            }

            RawList = RawList.OrderBy(l => l.TableName).ToList();

            // 合并同一个MES数据库的InsertRecord结构
            foreach (InsertRecord item in RawList) {
                bool NeedInsert = true;
                foreach (InsertRecord ExistItem in InsertList) {
                    if (ExistItem.DBName == item.DBName && ExistItem.TableName == item.TableName && ExistItem.Dic["VIN"] == item.Dic["VIN"]) {
                        foreach (var DicItem in item.Dic) {
                            if (!ExistItem.Dic.Keys.Contains(DicItem.Key)) {
                                ExistItem.Dic.Add(DicItem.Key, DicItem.Value);
                                NeedInsert = false;
                            } else if (DicItem.Value != "" && DicItem.Value != "0" && DicItem.Value != "0.0") {
                                ExistItem.Dic[DicItem.Key] = DicItem.Value;
                                NeedInsert = false;
                            }
                        }
                    }
                }
                if (NeedInsert) {
                    InsertList.Add(item);
                }
            }

            return InsertList;
        }

        /// <summary>
        /// 将InsertList中的数据插入到MES数据库中，同时更新LastIDContainList数据
        /// </summary>
        /// <param name="InsertList">包含可以插入MES数据库中的数据结构</param>
        /// <returns></returns>
        static int InsertNLDBToMESDB(List<InsertRecord> InsertList) {
            // 将InsertList中的数据插入MES数据库中
            int count = 0;
            foreach (InsertRecord item in InsertList) {
                if (item.TableName != "" && item.DBName != "") {
                    if (item.TableName == "FS_MES_Vehicle_TEST") {
                        // 对FS_MES_Vehicle_TEST表中的LEAVEFACTORYTIME、TESTTIME、TESTNUMBER字段做特殊处理
                        string[,] rs = db.GetRecords(item.TableName, "VIN", item.Dic["VIN"], item.DBName);
                        if (rs != null && rs.GetLength(0) > 0) {
                            int TestNumber = 0;
                            string[] col = db.GetTableColumns(item.TableName, item.DBName);
                            for (int i = 0; i < col.Length; i++) {
                                if (col[i] == "TESTNUMBER") {
                                    int.TryParse(rs[0, i], out int number);
                                    TestNumber = number;
                                }
                            }
                            if (item.Dic.Keys.Contains("TESTNUMBER")) {
                                int.TryParse(item.Dic["TESTNUMBER"], out int result);
                                TestNumber += result;
                                item.Dic["TESTNUMBER"] = TestNumber.ToString();
                            }
                            if (item.Dic.Keys.Contains("TESTTIME")) {
                                item.Dic["TESTTIME"] = DateTime.Now.ToLocalTime().ToString();
                            }
                            if (item.Dic.Keys.Contains("LEAVEFACTORYTIME")) {
                                item.Dic["LEAVEFACTORYTIME"] = DateTime.Now.ToLocalTime().ToString();
                            }
                            KeyValuePair<string, string> pairWhere = new KeyValuePair<string, string>("VIN", item.Dic["VIN"]);
                            count += db.UpdateRecord(item.TableName, pairWhere, item.Dic, item.DBName);
                        } else {
                            count += db.InsertRecord(item.TableName, item.Dic, item.DBName);
                        }
                    } else if (item.TableName == "FS_Vehicleinfo") {
                        // 对FS_Vehicleinfo表做特殊处理，该表只同步Z_B字段
                        string[,] rs = db.GetRecords(item.TableName, "VIN", item.Dic["VIN"], item.DBName);
                        if (rs != null && rs.GetLength(0) > 0) {
                            KeyValuePair<string, string> pairWhere = new KeyValuePair<string, string>("VIN", item.Dic["VIN"]);
                            count += db.UpdateRecord(item.TableName, pairWhere, item.Dic, item.DBName);
                        } else {
                            count += db.InsertRecord(item.TableName, item.Dic, item.DBName);
                        }
                    } else {
                        count += db.InsertRecord(item.TableName, item.Dic, item.DBName);
                    }
                }
            }

            Console.WriteLine("{0} - {1} records sync into MES database", DateTime.Now.ToLocalTime().ToString(), count);
            return count;
        }

        /// <summary>
        /// 获取MES数据库中的新数据，返回一个List<InsertRecord>结构包含有可以插入New Line数据库中的数据
        /// </summary>
        /// <returns></returns>
        static List<InsertRecord> GetNewMESDBData() {
            List<InsertRecord> InsertList = new List<InsertRecord>();
            // 生成新数据记录List
            List<Dictionary<string, string>> NewList = new List<Dictionary<string, string>>();
            for (int i = cfg.DB.MESDBStartIndex; i < cfg.DBInfoList.Count; i++) {
                List<int> LastIDList = new List<int>();
                int len = cfg.DBInfoList[i].TableList.Count;
                if (cfg.DBInfoList[i].LastIDList.Count == len) {
                    string DBName = cfg.DBInfoList[i].Name;
                    for (int j = 0; j < len; j++) {
                        string TableName = cfg.DBInfoList[i].TableList[j];
                        string[,] rs;
                        // 对FS_Vehicleinfo表的主键做特殊处理
                        if (TableName == "FS_Vehicleinfo") {
                            rs = db.GetNewRecords(TableName, "ID", cfg.DBInfoList[i].LastIDList[j].ToString(), DBName);
                        } else {
                            rs = db.GetNewRecords(TableName, "SELFID", cfg.DBInfoList[i].LastIDList[j].ToString(), DBName);
                        }
                        if (rs != null) {
                            int rowNum = rs.GetLength(0);
                            string[] colNew = db.GetTableColumns(TableName, DBName);
                            int IDIndex = 0;
                            if (rowNum > 0) {
                                for (int n = 0; n < rowNum; n++) {
                                    Dictionary<string, string> dic = new Dictionary<string, string>();
                                    for (int k = 0; k < colNew.Length; k++) {
                                        if (!cfg.DB.IDColList.Contains(colNew[k])) {
                                            dic.Add(DBName + "." + TableName + "." + colNew[k], rs[n, k]);
                                        } else {
                                            IDIndex = k;
                                        }
                                    }
                                    NewList.Add(dic);
                                }
                            }
                        }
                    }
                } else {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("ERROR: \"LastIDList\" in MES \"DBInfo\" error");
                    Console.ResetColor();
                    log.TraceError("\"LastIDList\" in MES \"DBInfo\" error");
                    return InsertList;
                }
            }

            // 生成需插入数据记录的List
            List<Dictionary<string, string>> InsertNLList = new List<Dictionary<string, string>>();
            foreach (Dictionary<string, string> listItem in NewList) {
                SortedDictionary<string, string> dicSortNL = new SortedDictionary<string, string>();
                foreach (KeyValuePair<string, string> dicItem in listItem) {
                    if (dicItem.Key.EndsWith(".VIN")) {
                        dicSortNL.Add("VIN", dicItem.Value);
                    }
                    if (cfg.SyncDicMES.ContainsKey(dicItem.Key) && cfg.SyncDicMES[dicItem.Key] != "") {
                        dicSortNL.Add(cfg.SyncDicMES[dicItem.Key], dicItem.Value);
                    }
                }
                if (dicSortNL.Count > 1) {
                    // 处理MES对New一对多的情况
                    // 根据key将一条Dictionary分成多条
                    Dictionary<string, string> dicSolo = new Dictionary<string, string>();
                    string keyName = "";
                    foreach (KeyValuePair<string, string> item in dicSortNL) {
                        string[] key = item.Key.Split('.');
                        if (item.Key != "VIN") {
                            if (keyName != key[0] + "." + key[1] && keyName != "") {
                                dicSolo.Add(keyName + ".VIN", dicSortNL["VIN"]);
                                InsertNLList.Add(dicSolo);
                                dicSolo = new Dictionary<string, string>();
                            }
                            dicSolo.Add(item.Key, item.Value);
                            keyName = key[0] + "." + key[1];
                        }
                    }
                    dicSolo.Add(keyName + ".VIN", dicSortNL["VIN"]);
                    InsertNLList.Add(dicSolo);
                }
            }

            // 生成InsertRecord结构List
            List<InsertRecord> RawList = new List<InsertRecord>();
            foreach (Dictionary<string, string> dic in InsertNLList) {
                InsertRecord ir = new InsertRecord {
                    DBName = "",
                    TableName = ""
                };
                Dictionary<string, string> dicInsert = new Dictionary<string, string>();
                foreach (KeyValuePair<string, string> item in dic) {
                    string[] keyArray = item.Key.Split('.');
                    if (ir.TableName == "" && ir.DBName == "") {
                        ir.DBName = keyArray[0];
                        ir.TableName = keyArray[1];
                    }
                    // 因New Line数据库的数据格式均为数字，故需要过滤不能转换成数字的非法字符
                    if (item.Value == "-" || item.Value == "X" || item.Value == "x") {
                        dicInsert.Add(keyArray[2], "0");
                    } else if (item.Value == "O" || item.Value == "o") {
                        dicInsert.Add(keyArray[2], "1");
                    } else {
                        dicInsert.Add(keyArray[2], item.Value);
                    }
                }
                ir.Dic = dicInsert;
                RawList.Add(ir);
            }

            RawList = RawList.OrderBy(l => l.TableName).ToList();

            // 合并同一个New Line数据库的InsertRecord结构
            foreach (InsertRecord item in RawList) {
                bool NeedInsert = true;
                foreach (InsertRecord ExistItem in InsertList) {
                    if (ExistItem.DBName == item.DBName && ExistItem.TableName == item.TableName && ExistItem.Dic["VIN"] == item.Dic["VIN"]) {
                        foreach (var DicItem in item.Dic) {
                            if (!ExistItem.Dic.Keys.Contains(DicItem.Key)) {
                                ExistItem.Dic.Add(DicItem.Key, DicItem.Value);
                                NeedInsert = false;
                            } else if (DicItem.Value != "" && DicItem.Value != "0" && DicItem.Value != "0.0") {
                                ExistItem.Dic[DicItem.Key] = DicItem.Value;
                                NeedInsert = false;
                            }
                        }
                    }
                }
                if (NeedInsert) {
                    InsertList.Add(item);
                }
            }

            return InsertList;
        }

        /// <summary>
        /// 将InsertList中的数据插入到New Line数据库中，同时更新LastIDContainList数据
        /// </summary>
        /// <param name="InsertList">包含可以插入New Line数据库中的数据结构</param>
        /// <returns></returns>
        static int InsertMESDBToNLDB(List<InsertRecord> InsertList) {
            // 将InsertList中的数据插入MES数据库中
            int count = 0;
            foreach (InsertRecord item in InsertList) {
                if (item.TableName != "" && item.DBName != "") {
                    if (item.TableName == "VehicleInfo") {
                        // 对VehicleInfo表中的TestNumber字段做特殊处理
                        string[,] rs = db.GetRecords(item.TableName, "VIN", item.Dic["VIN"], item.DBName);
                        if (rs != null && rs.GetLength(0) > 0) {
                            int TestNumber = 0;
                            string[] col = db.GetTableColumns(item.TableName, item.DBName);
                            for (int i = 0; i < col.Length; i++) {
                                if (col[i] == "TestNumber") {
                                    int.TryParse(rs[0, i], out int number);
                                    TestNumber = number;
                                }
                            }
                            if (item.Dic.Keys.Contains("TestNumber")) {
                                int.TryParse(item.Dic["TestNumber"], out int result);
                                TestNumber += result;
                                item.Dic["TestNumber"] = TestNumber.ToString();
                            }
                            if (item.Dic.Keys.Contains("TestTime")) {
                                item.Dic["TestTime"] = DateTime.Now.ToLocalTime().ToString();
                            }
                            if (item.Dic.Keys.Contains("LeaveFactoryTime")) {
                                item.Dic["LeaveFactoryTime"] = DateTime.Now.ToLocalTime().ToString();
                            }
                            KeyValuePair<string, string> pairWhere = new KeyValuePair<string, string>("VIN", item.Dic["VIN"]);
                            count += db.UpdateRecord(item.TableName, pairWhere, item.Dic, item.DBName);
                        } else {
                            count += db.InsertRecord(item.TableName, item.Dic, item.DBName);
                        }
                    } else {
                        count += db.InsertRecord(item.TableName, item.Dic, item.DBName);
                    }
                }
            }

            Console.WriteLine("{0} - {1} records sync into New Line database", DateTime.Now.ToLocalTime().ToString(), count);
            return count;
        }

        /// <summary>
        /// 获取New Line数据库中LastID
        /// </summary>
        /// <param name="LastIDContainList">数据库中包含LastIDList的容器List</param>
        static void GetNLLastID(out List<List<int>> LastIDContainList) {
            LastIDContainList = new List<List<int>>();
            for (int i = 0; i < cfg.DB.MESDBStartIndex; i++) {
                List<int> LastIDList = new List<int>();
                int len = cfg.DBInfoList[i].TableList.Count;
                if (cfg.DBInfoList[i].LastIDList.Count == len) {
                    string DBName = cfg.DBInfoList[i].Name;
                    for (int j = 0; j < len; j++) {
                        int LastID = cfg.DBInfoList[i].LastIDList[j];
                        string TableName = cfg.DBInfoList[i].TableList[j];
                        string[,] rs = db.GetNewRecords(TableName, "ID", cfg.DBInfoList[i].LastIDList[j].ToString(), DBName);
                        if (rs != null) {
                            int rowNum = rs.GetLength(0);
                            string[] col = db.GetTableColumns(TableName, DBName);
                            if (rowNum > 0) {
                                for (int k = 0; k < col.Length; k++) {
                                    if (cfg.DB.IDColList.Contains(col[k])) {
                                        int.TryParse(rs[rowNum - 1, k], out int ID);
                                        LastID = ID;
                                        break;
                                    }
                                }
                            }
                        }
                        LastIDList.Add(LastID);
                    }
                    LastIDContainList.Add(LastIDList);
                }
            }
        }

        /// <summary>
        /// 获取MES数据库中LastID
        /// </summary>
        /// <param name="LastIDContainList">数据库中包含LastIDList的容器List</param>
        static void GetMESLastID(List<List<int>> LastIDContainList) {
            for (int i = cfg.DB.MESDBStartIndex; i < cfg.DBInfoList.Count; i++) {
                List<int> LastIDList = new List<int>();
                int len = cfg.DBInfoList[i].TableList.Count;
                if (cfg.DBInfoList[i].LastIDList.Count == len) {
                    string DBName = cfg.DBInfoList[i].Name;
                    for (int j = 0; j < len; j++) {
                        int LastID = cfg.DBInfoList[i].LastIDList[j];
                        string TableName = cfg.DBInfoList[i].TableList[j];
                        string[,] rs;
                        // 对FS_Vehicleinfo表的主键做特殊处理
                        if (TableName == "FS_Vehicleinfo") {
                            rs = db.GetNewRecords(TableName, "ID", cfg.DBInfoList[i].LastIDList[j].ToString(), DBName);
                        } else {
                            rs = db.GetNewRecords(TableName, "SELFID", cfg.DBInfoList[i].LastIDList[j].ToString(), DBName);
                        }
                        if (rs != null) {
                            int rowNum = rs.GetLength(0);
                            string[] col = db.GetTableColumns(TableName, DBName);
                            if (rowNum > 0) {
                                for (int k = 0; k < col.Length; k++) {
                                    if (cfg.DB.IDColList.Contains(col[k])) {
                                        int.TryParse(rs[rowNum - 1, k], out int ID);
                                        LastID = ID;
                                        break;
                                    }
                                }
                            }
                        }
                        LastIDList.Add(LastID);
                    }
                    LastIDContainList.Add(LastIDList);
                }
            }
        }

        /// <summary>
        /// 更新LastID值
        /// </summary>
        /// <param name="LastIDContainList">数据库中包含LastIDList的容器List</param>
        static void UpdateLastID(List<List<int>> LastIDContainList) {
            // 修改LastID值
            for (int i = 0; i < cfg.DBInfoList.Count; i++) {
                DBInfoConfig TempDBInfo = cfg.DBInfoList[i];
                TempDBInfo.LastIDList = LastIDContainList[i];
                cfg.DBInfoList[i] = TempDBInfo;
            }
            cfg.SaveConfig();
        }

    }
}
