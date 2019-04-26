using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using static MSSQL_Sync.Config;

namespace MSSQL_Sync {
    public class Model {
        public Dictionary<string, string> connDic;
        readonly Logger log;
        readonly Config cfg;

        public Model(Config cfg, Logger log) {
            this.cfg = cfg;
            this.log = log;
            this.connDic = new Dictionary<string, string>();
            ReadDBInfo();
        }

        void ReadDBInfo() {
            for (int i = 0; i < cfg.DBInfoList.Count; i++) {
                string str = "user id=" + cfg.DBInfoList[i].UserID + ";";
                str += "password=" + cfg.DBInfoList[i].Pwd + ";";
                str += "database=" + cfg.DBInfoList[i].Name + ";";
                str += "data source=" + cfg.DBInfoList[i].IP + "," + cfg.DBInfoList[i].Port;
                this.connDic.Add(cfg.DBInfoList[i].Name, str);
            }
        }

        public void ShowDB(string StrTable, string DBName) {
            string StrSQL = "select * from " + StrTable;

            using (SqlConnection sqlConn = new SqlConnection(connDic[DBName])) {
                sqlConn.Open();
                SqlCommand sqlCmd = new SqlCommand(StrSQL, sqlConn);
                SqlDataReader sqlData = sqlCmd.ExecuteReader();
                string str = "";
                int c = sqlData.FieldCount;
                while (sqlData.Read()) {
                    for (int i = 0; i < c; i++) {
                        object obj = sqlData.GetValue(i);
                        if (obj.GetType() == typeof(DateTime)) {
                            str += ((DateTime)obj).ToString("yyyy-MM-dd") + "\t";
                        } else {
                            str += obj.ToString() + "\t";
                        }
                    }
                    str += "\n";
                }
                Console.WriteLine(str);
                sqlConn.Close();
            }
        }

        public string[] GetTableName(string DBName) {
            try {
                using (SqlConnection sqlConn = new SqlConnection(connDic[DBName])) {
                    sqlConn.Open();
                    DataTable schema = sqlConn.GetSchema("Tables");
                    int count = schema.Rows.Count;
                    string[] tableName = new string[count];
                    for (int i = 0; i < count; i++) {
                        DataRow row = schema.Rows[i];
                        foreach (DataColumn col in schema.Columns) {
                            if (col.Caption == "TABLE_NAME") {
                                if (col.DataType.Equals(typeof(DateTime))) {
                                    tableName[i] = string.Format("{0:d}", row[col]);
                                } else if (col.DataType.Equals(typeof(Decimal))) {
                                    tableName[i] = string.Format("{0:C}", row[col]);
                                } else {
                                    tableName[i] = string.Format("{0}", row[col]);
                                }
                            }
                        }
                    }
                    sqlConn.Close();
                    return tableName;
                }
            } catch (Exception e) {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: " + e.Message);
                Console.ResetColor();
                log.TraceError(e.Message);
            }
            return new string[] { "" };
        }

        public string[] GetTableColumns(string strTableName, string DBName) {
            try {
                using (SqlConnection sqlConn = new SqlConnection(connDic[DBName])) {
                    sqlConn.Open();
                    DataTable schema = sqlConn.GetSchema("Columns", new string[] { null, null, strTableName });
                    schema.DefaultView.Sort = "ORDINAL_POSITION";
                    schema = schema.DefaultView.ToTable();
                    int count = schema.Rows.Count;
                    string[] ColumnName = new string[count];
                    for (int i = 0; i < count; i++) {
                        DataRow row = schema.Rows[i];
                        foreach (DataColumn col in schema.Columns) {
                            if (col.Caption == "COLUMN_NAME") {
                                if (col.DataType.Equals(typeof(DateTime))) {
                                    ColumnName[i] = string.Format("{0:d}", row[col]);
                                } else if (col.DataType.Equals(typeof(Decimal))) {
                                    ColumnName[i] = string.Format("{0:C}", row[col]);
                                } else {
                                    ColumnName[i] = string.Format("{0}", row[col]);
                                }
                            }
                        }
                    }
                    sqlConn.Close();
                    return ColumnName;
                }
            } catch (Exception e) {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: " + e.Message);
                Console.ResetColor();
                log.TraceError(e.Message);
            }
            return new string[] { "" };
        }

        public int GetRecordsCount(string strTableName, string DBName) {
            string strSQL = "select count(*) from " + strTableName;
            log.TraceInfo("SQL: " + strSQL);
            int count = 0;
            try {
                using (SqlConnection sqlConn = new SqlConnection(connDic[DBName])) {
                    SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                    sqlConn.Open();
                    count = (int)sqlCmd.ExecuteScalar();
                    sqlConn.Close();
                }
            } catch (Exception e) {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: " + e.Message);
                Console.WriteLine("ERROR: " + strSQL);
                Console.ResetColor();
                log.TraceError(e.Message);
                log.TraceError(strSQL);
            }
            return count;
        }

        public int ModifyDB(string strTableName, string[] strID, string[,] strValue, string DBName) {
            int iRet = 0;
            int iRowNum = strValue.GetLength(0);
            int iColNum = strValue.GetLength(1);
            int iIDNum = strID.Length;
            log.TraceInfo(string.Format("iRowNum:{0}, iIDNum:{1}", iRowNum, iIDNum));
            if (iRowNum == iIDNum) {
                iRet += UpdateDB(strTableName, strID, strValue, DBName);
            } else if (iRowNum > iIDNum) {
                string[,] strUpdate = new string[iIDNum, iColNum];
                Array.Copy(strValue, 0, strUpdate, 0, iIDNum * iColNum);
                iRet += UpdateDB(strTableName, strID, strUpdate, DBName);

                string[,] strInsert = new string[iRowNum - iIDNum, iColNum];
                Array.Copy(strValue, iIDNum * iColNum, strInsert, 0, (iRowNum - iIDNum) * iColNum);
                iRet += InsertDB(strTableName, strInsert, DBName);
            } else {
                iRet = -1;
            }
            return iRet;
        }

        int UpdateDB(string strTableName, string[] strID, string[,] strValue, string DBName) {
            int iRet = 0;
            int iRowNum = strValue.GetLength(0);
            int iColNum = strValue.GetLength(1);
            if (iRowNum * strID.Length == 0) {
                return -1;
            }
            string strSQL = "";
            string[] strColumns = GetTableColumns(strTableName, DBName);
            try {
                using (SqlConnection sqlConn = new SqlConnection(connDic[DBName])) {
                    sqlConn.Open();
                    for (int i = 0; i < iRowNum; i++) {
                        strSQL = "update ";
                        string strSet = "set ";
                        for (int j = 1; j < strColumns.Length; j++) {
                            strSet += strColumns[j] + " = '" + strValue[i, j - 1] + "', ";
                        }
                        strSet = strSet.Remove(strSet.Length - 2);
                        strSQL += string.Format("{0} {1} where ID = '{2}'", strTableName, strSet, strID[i]);
                        log.TraceInfo("SQL: " + strSQL);
                        SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                        iRet += sqlCmd.ExecuteNonQuery();
                    }
                    sqlConn.Close();
                }
            } catch (Exception e) {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: " + e.Message);
                Console.WriteLine("ERROR: " + strSQL);
                Console.ResetColor();
                log.TraceError(e.Message);
                log.TraceError(strSQL);
            }
            return iRet;
        }

        int InsertDB(string strTableName, string[,] strValue, string DBName) {
            int iRet = 0;
            int iRowNum = strValue.GetLength(0);
            int iColNum = strValue.GetLength(1);
            if (iRowNum * iColNum == 0) {
                return -1;
            }
            string strSQL = "";
            try {
                using (SqlConnection sqlConn = new SqlConnection(connDic[DBName])) {
                    sqlConn.Open();
                    for (int i = 0; i < iRowNum; i++) {
                        strSQL = "insert " + strTableName + " values (";
                        for (int j = 0; j < iColNum; j++) {
                            strSQL += "'" + strValue[i, j] + "', ";
                        }
                        strSQL = strSQL.Remove(strSQL.Length - 2);
                        strSQL += ")";
                        log.TraceInfo("SQL: " + strSQL);
                        SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                        iRet += sqlCmd.ExecuteNonQuery();
                    }
                    sqlConn.Close();
                }
            } catch (Exception e) {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: " + e.Message);
                Console.WriteLine("ERROR: " + strSQL);
                Console.ResetColor();
                log.TraceError(e.Message);
                log.TraceError(strSQL);
            }
            return iRet;
        }

        public int InsertRecord(string strTableName, Dictionary<string, string> dicValue, string DBName) {
            int iRet = 0;
            int iColNum = dicValue.Count;
            if (iColNum <= 0) {
                return -1;
            }
            string strSQL = "";
            try {
                using (SqlConnection sqlConn = new SqlConnection(connDic[DBName])) {
                    sqlConn.Open();
                    strSQL = "insert " + strTableName + " (";
                    foreach (string key in dicValue.Keys) {
                        strSQL += key + ", ";
                    }
                    strSQL = strSQL.Remove(strSQL.Length - 2);
                    strSQL += ")";
                    strSQL += " values (";
                    foreach (string value in dicValue.Values) {
                        strSQL += "'" + value + "', ";
                    }
                    strSQL = strSQL.Remove(strSQL.Length - 2);
                    strSQL += ")";
                    log.TraceInfo("SQL: " + strSQL);
                    SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                    iRet = sqlCmd.ExecuteNonQuery();
                    sqlConn.Close();
                }
            } catch (Exception e) {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: " + e.Message);
                Console.WriteLine("ERROR: " + strSQL);
                Console.ResetColor();
                log.TraceError(e.Message);
                log.TraceError(strSQL);
            }
            return iRet;
        }

        public int UpdateRecord(string strTableName, KeyValuePair<string, string> pairWhere, Dictionary<string, string> dicValueSet, string DBName) {
            int iRet = 0;
            int iColNum = dicValueSet.Count;
            if (iColNum <= 0) {
                return -1;
            }
            string strSQL = "";
            try {
                using (SqlConnection sqlConn = new SqlConnection(connDic[DBName])) {
                    sqlConn.Open();
                    strSQL = "update " + strTableName + " set ";
                    foreach (var item in dicValueSet) {
                        strSQL += item.Key + " = '" + item.Value + "', ";
                    }
                    strSQL = strSQL.Remove(strSQL.Length - 2);
                    strSQL += " where " + pairWhere.Key + " = '" + pairWhere.Value + "'";
                    log.TraceInfo("SQL: " + strSQL);
                    SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                    iRet = sqlCmd.ExecuteNonQuery();
                    sqlConn.Close();
                }
            } catch (Exception e) {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: " + e.Message);
                Console.WriteLine("ERROR: " + strSQL);
                Console.ResetColor();
                log.TraceError(e.Message);
                log.TraceError(strSQL);
            }
            return iRet;
        }

        public int DeleteDB(string strTableName, string[] strID, string DBName) {
            int iRet = 0;
            int length = strID.Length;
            string strSQL = "";
            try {
                using (SqlConnection sqlConn = new SqlConnection(connDic[DBName])) {
                    sqlConn.Open();
                    for (int i = 0; i < length; i++) {
                        strSQL = "delete from " + strTableName + " where ID = '" + strID[i] + "'";
                        log.TraceInfo("SQL: " + strSQL);
                        SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                        iRet += sqlCmd.ExecuteNonQuery();
                    }
                    sqlConn.Close();
                }
            } catch (Exception e) {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: " + e.Message);
                Console.WriteLine("ERROR: " + strSQL);
                Console.ResetColor();
                log.TraceError(e.Message);
                log.TraceError(strSQL);
            }
            return iRet;
        }

        string[,] SelectDB(string strSQL, string DBName) {
            string[,] records = null;
            try {
                int count = 0;
                List<string[]> rowList;
                using (SqlConnection sqlConn = new SqlConnection(connDic[DBName])) {
                    SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                    sqlConn.Open();
                    SqlDataReader sqlData = sqlCmd.ExecuteReader();
                    count = sqlData.FieldCount;
                    rowList = new List<string[]>();
                    while (sqlData.Read()) {
                        string[] items = new string[count];
                        for (int i = 0; i < count; i++) {
                            object obj = sqlData.GetValue(i);
                            if (obj.GetType() == typeof(DateTime)) {
                                items[i] = ((DateTime)obj).ToString("yyyy-MM-dd HH:mm:ss");
                            } else {
                                items[i] = obj.ToString();
                            }
                        }
                        rowList.Add(items);
                    }
                    sqlConn.Close();
                }
                records = new string[rowList.Count, count];
                for (int i = 0; i < rowList.Count; i++) {
                    for (int j = 0; j < count; j++) {
                        records[i, j] = rowList[i][j];
                    }
                }
                return records;
            } catch (Exception e) {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: " + e.Message);
                Console.WriteLine("ERROR: " + strSQL);
                Console.ResetColor();
                log.TraceError(e.Message);
                log.TraceError(strSQL);
            }
            return records;
        }

        public string[,] GetLikeRecords(string strTableName, string strColumn, string strValue, string DBName) {
            string strSQL = "select " + strColumn + " from " + strTableName + " where " + strColumn + " like '%" + strValue + "%'";
            log.TraceInfo("SQL: " + strSQL);
            string[,] strArr = SelectDB(strSQL, DBName);
            return strArr;
        }

        public string[,] GetRecords(string strTableName, string strColumn, string strValue, string DBName) {
            string strSQL = "select * from " + strTableName + " where " + strColumn + " = '" + strValue + "'";
            log.TraceInfo("SQL: " + strSQL);
            string[,] strArr = SelectDB(strSQL, DBName);
            return strArr;
        }

        public string[,] GetRecordsOneCol(string strTableName, string strSelectCol, string strWhereCol, string strWhereValue, string DBName) {
            string strSQL = "select " + strSelectCol + " from " + strTableName + " where " + strWhereCol + " = '" + strWhereValue + "'";
            log.TraceInfo("SQL: " + strSQL);
            string[,] strArr = SelectDB(strSQL, DBName);
            return strArr;
        }

        public string[,] GetNewRecords(string strTableName, string IDColName, string LastID, string DBName) {
            string strSQL = "select * from " + strTableName + " where " + IDColName + " > '" + LastID + "'";
            log.TraceInfo("SQL: " + strSQL);
            string[,] strArr = SelectDB(strSQL, DBName);
            return strArr;
        }
    }
}
