/*
 * Copyright © 2008 Daniel W. Goldberg
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

using Microsoft.SqlServer.Types;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using USC.GISResearchLab.AddressProcessing.Core.Standardizing.StandardizedAddresses.Lines.LastLines;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Databases.SchemaManagers;
using USC.GISResearchLab.Common.Diagnostics.TraceEvents;

namespace TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.Workers
{
    public class SqlServerCensusTractLevelCountsUpdaterImporterWorker : AbstractCensusTractLevelCountsUpdaterImporterWorker
    {
        #region Properties

        #endregion

        public SqlServerCensusTractLevelCountsUpdaterImporterWorker() : base() { }

        public SqlServerCensusTractLevelCountsUpdaterImporterWorker(TraceSource traceSource, BackgroundWorker backgroundWorker, IQueryManager outputDataQueryManager, ISchemaManager schemaManager) : base(traceSource, backgroundWorker, outputDataQueryManager, schemaManager) { }



        public override void UpdateZIPCT2000Counts()
        {
            string sql = "";
            try
            {

                if (Zips != null)
                {

                    foreach (string zip in Zips)
                    {
                        if (!BackgroundWorker.CancellationPending)
                        {
                            TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "Update Tiger2000 ZIP Census Tract Counts: " + zip + "% - (" + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + ")");

                            sql += " use Tiger2010CountryFiles ";
                            sql += " Select zcta5ce10, shapeGeog from us_zcta510 ";
                            sql += " where zcta5ce10 like '" + zip + "%' ";

                            SqlCommand cmd = new SqlCommand(sql);
                            IQueryManager qm = QueryManager;
                            qm.AddParameters(cmd.Parameters);
                            DataTable dataTable = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, 6000, true);

                            if (dataTable != null && dataTable.Rows.Count > 0)
                            {
                                ProgressState.ProgressStateRecords.Total = dataTable.Rows.Count;
                                ProgressState.ProgressStateRecords.Completed = 0;
                                TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "got records : " + dataTable.Rows.Count);

                                int i = 1;

                                foreach (DataRow dataRow in dataTable.Rows)
                                {
                                    if (!BackgroundWorker.CancellationPending)
                                    {
                                        List<string> intersectingStates = new List<string>();
                                        int totalIntersections = 0;

                                        string id = (String)dataRow["zcta5ce10"];

                                        if (dataRow["shapeGeog"] != null && dataRow["shapeGeog"] != DBNull.Value)
                                        {
                                            SqlGeography sqlGeography = (SqlGeography)dataRow["shapeGeog"];

                                            if (sqlGeography != null)
                                            {

                                                ProgressState.ProgressStateRecords.Completed = i;
                                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.ProgressStateRecords.PercentCompleted), ProgressState);

                                                string stateName1 = StateUtils.GetStateByZIP3Digit(id.Substring(0, 3));

                                                if (StateUtils.isState(stateName1))
                                                {
                                                    sql = "";
                                                    sql += " use Tiger2000CensusTracts ";
                                                    sql += " IF  EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + stateName1 + "_tract00') ";
                                                    sql += " SELECT COUNT(CtidFp00) ";
                                                    sql += " FROM  " + stateName1 + "_tract00 ";
                                                    sql += " WITH (INDEX (idx_geog))";
                                                    sql += " WHERE shapeGeog.STIntersects(@shapeGeog) = 1";

                                                    cmd = new SqlCommand(sql);

                                                    SqlParameter parameter = new SqlParameter("shapeGeog", sqlGeography);

                                                    parameter.Direction = ParameterDirection.Input;
                                                    parameter.ParameterName = "shapeGeog";
                                                    parameter.SourceColumn = "shapeGeog";
                                                    parameter.SqlDbType = SqlDbType.Udt;
                                                    parameter.UdtTypeName = "GEOGRAPHY";
                                                    parameter.SourceVersion = DataRowVersion.Current;

                                                    cmd.Parameters.Add(parameter);


                                                    qm = QueryManager;
                                                    qm.AddParameters(cmd.Parameters);
                                                    int intersections = qm.ExecuteScalarInt(CommandType.Text, cmd.CommandText, 6000, true);

                                                    if (intersections >= 0)
                                                    {
                                                        totalIntersections += intersections;

                                                        if (intersections > 0)
                                                        {
                                                            intersectingStates.Add(stateName1);
                                                        }

                                                    }
                                                    else
                                                    {
                                                        string here = "";
                                                    }
                                                }



                                                i++;
                                            }

                                            if (!BackgroundWorker.CancellationPending)
                                            {
                                                if (totalIntersections > 0)
                                                {
                                                    sql = " use Tiger2010CountryFiles ";
                                                    sql += " update us_zcta510";
                                                    sql += " set num2000TractsIntersect = " + totalIntersections;
                                                    sql += " WHERE zcta5ce10 = '" + id + "'";

                                                    cmd = new SqlCommand(sql);
                                                    qm = QueryManager;
                                                    qm.AddParameters(cmd.Parameters);
                                                    qm.ExecuteNonQuery(CommandType.Text, cmd.CommandText, 6000, true);
                                                }
                                            }

                                        }
                                    }
                                    else
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Error occured in UpdateZIPCT2010Counts: " + e.Message, e);
            }
        }

        public override void UpdateZIPCT2010Counts()
        {
            string sql = "";
            try
            {

                if (Zips != null)
                {

                    foreach (string zip in Zips)
                    {
                        if (!BackgroundWorker.CancellationPending)
                        {
                            TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "Update Tiger2010 ZIP Census Tract Counts: " + zip + "% - (" + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + ")");

                            sql += " use Tiger2010CountryFiles ";
                            sql += " Select zcta5ce10, shapeGeog from us_zcta510 ";
                            sql += " where zcta5ce10 like '" + zip + "%' ";

                            SqlCommand cmd = new SqlCommand(sql);
                            IQueryManager qm = QueryManager;
                            qm.AddParameters(cmd.Parameters);
                            DataTable dataTable = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, 6000, true);

                            if (dataTable != null && dataTable.Rows.Count > 0)
                            {
                                ProgressState.ProgressStateRecords.Total = dataTable.Rows.Count;
                                ProgressState.ProgressStateRecords.Completed = 0;
                                TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "got records : " + dataTable.Rows.Count);

                                int i = 1;

                                foreach (DataRow dataRow in dataTable.Rows)
                                {
                                    if (!BackgroundWorker.CancellationPending)
                                    {
                                        List<string> intersectingStates = new List<string>();
                                        int totalIntersections = 0;

                                        string id = (String)dataRow["zcta5ce10"];

                                        if (dataRow["shapeGeog"] != null && dataRow["shapeGeog"] != DBNull.Value)
                                        {
                                            SqlGeography sqlGeography = (SqlGeography)dataRow["shapeGeog"];

                                            if (sqlGeography != null)
                                            {

                                                ProgressState.ProgressStateRecords.Completed = i;
                                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.ProgressStateRecords.PercentCompleted), ProgressState);

                                                string stateName1 = StateUtils.GetStateByZIP3Digit(id.Substring(0, 3));

                                                if (StateUtils.isState(stateName1))
                                                {
                                                    sql = "";
                                                    sql += " use Tiger2010CensusTracts ";
                                                    sql += " IF  EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + stateName1 + "_tract10') ";
                                                    sql += " SELECT COUNT(uniqueId) ";
                                                    sql += " FROM  " + stateName1 + "_tract10 ";
                                                    sql += " WITH (INDEX (idx_geog))";
                                                    sql += " WHERE shapeGeog.STIntersects(@shapeGeog) = 1";

                                                    cmd = new SqlCommand(sql);

                                                    SqlParameter parameter = new SqlParameter("shapeGeog", sqlGeography);

                                                    parameter.Direction = ParameterDirection.Input;
                                                    parameter.ParameterName = "shapeGeog";
                                                    parameter.SourceColumn = "shapeGeog";
                                                    parameter.SqlDbType = SqlDbType.Udt;
                                                    parameter.UdtTypeName = "GEOGRAPHY";
                                                    parameter.SourceVersion = DataRowVersion.Current;

                                                    cmd.Parameters.Add(parameter);


                                                    qm = QueryManager;
                                                    qm.AddParameters(cmd.Parameters);
                                                    int intersections = qm.ExecuteScalarInt(CommandType.Text, cmd.CommandText, 6000, true);

                                                    if (intersections >= 0)
                                                    {
                                                        totalIntersections += intersections;

                                                        if (intersections > 0)
                                                        {
                                                            intersectingStates.Add(stateName1);
                                                        }

                                                    }
                                                    else
                                                    {
                                                        string here = "";
                                                    }
                                                }



                                                i++;
                                            }

                                            if (!BackgroundWorker.CancellationPending)
                                            {
                                                if (totalIntersections > 0)
                                                {
                                                    sql = " use Tiger2010CountryFiles ";
                                                    sql += " update us_zcta510";
                                                    sql += " set num2010TractsIntersect = " + totalIntersections;
                                                    sql += " WHERE zcta5ce10 = '" + id + "'";

                                                    cmd = new SqlCommand(sql);
                                                    qm = QueryManager;
                                                    qm.AddParameters(cmd.Parameters);
                                                    qm.ExecuteNonQuery(CommandType.Text, cmd.CommandText, 6000, true);
                                                }
                                            }

                                        }
                                    }
                                    else
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Error occured in UpdateZIPCT2010Counts: " + e.Message, e);
            }
        }



        public override void UpdatePlaceCT2000Counts()
        {
            string sql = "";
            try
            {

                if (States != null)
                {

                    foreach (string state in States)
                    {
                        if (!BackgroundWorker.CancellationPending)
                        {
                            TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "Update Tiger2000 Place Census Tract Counts: " + state + " - (" + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + ")");

                            sql += " use Tiger2010StateFiles ";
                            sql += " IF  EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + state + "_place10') ";
                            sql += " ALTER TABLE  " + state + "_place10 ";
                            sql += " ADD num2000TractsIntersect int null default 0; ";

                            SqlCommand cmd = new SqlCommand(sql);
                            IQueryManager qm = QueryManager;
                            qm.AddParameters(cmd.Parameters);
                            qm.ExecuteNonQuery(CommandType.Text, cmd.CommandText, 6000, true);

                            sql = "";
                            sql += " use Tiger2010StateFiles ";
                            sql += " IF  EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + state + "_place10') ";
                            sql += " Select placeFp10, shapeGeog from " + state + "_place10; ";

                            cmd = new SqlCommand(sql);
                            qm = QueryManager;
                            qm.AddParameters(cmd.Parameters);
                            DataTable dataTable = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, 6000, true);

                            if (dataTable != null && dataTable.Rows.Count > 0)
                            {
                                ProgressState.ProgressStateRecords.Total = dataTable.Rows.Count;
                                ProgressState.ProgressStateRecords.Completed = 0;
                                TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "got records : " + dataTable.Rows.Count);

                                int i = 1;

                                foreach (DataRow dataRow in dataTable.Rows)
                                {
                                    if (!BackgroundWorker.CancellationPending)
                                    {
                                        List<string> intersectingStates = new List<string>();
                                        int totalIntersections = 0;

                                        string id = (String)dataRow["placeFp10"];

                                        if (dataRow["shapeGeog"] != null && dataRow["shapeGeog"] != DBNull.Value)
                                        {
                                            SqlGeography sqlGeography = (SqlGeography)dataRow["shapeGeog"];

                                            if (sqlGeography != null)
                                            {

                                                ProgressState.ProgressStateRecords.Completed = i;
                                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.ProgressStateRecords.PercentCompleted), ProgressState);

                                                sql = "";
                                                sql += " use Tiger2000CensusTracts ";
                                                sql += " IF  EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + state + "_tract00') ";
                                                sql += " SELECT COUNT(CtidFp00) ";
                                                sql += " FROM  " + state + "_tract00 ";
                                                sql += " WITH (INDEX (idx_geog))";
                                                sql += " WHERE shapeGeog.STIntersects(@shapeGeog) = 1 ;";

                                                cmd = new SqlCommand(sql);

                                                SqlParameter parameter = new SqlParameter("shapeGeog", sqlGeography);

                                                parameter.Direction = ParameterDirection.Input;
                                                parameter.ParameterName = "shapeGeog";
                                                parameter.SourceColumn = "shapeGeog";
                                                parameter.SqlDbType = SqlDbType.Udt;
                                                parameter.UdtTypeName = "GEOGRAPHY";
                                                parameter.SourceVersion = DataRowVersion.Current;

                                                cmd.Parameters.Add(parameter);


                                                qm = QueryManager;
                                                qm.AddParameters(cmd.Parameters);
                                                int intersections = qm.ExecuteScalarInt(CommandType.Text, cmd.CommandText, 6000, true);

                                                if (intersections >= 0)
                                                {
                                                    totalIntersections += intersections;

                                                    if (intersections > 0)
                                                    {
                                                        intersectingStates.Add(state);
                                                    }

                                                }
                                                else
                                                {
                                                    string here = "";
                                                }
                                            }



                                            i++;
                                        }

                                        if (!BackgroundWorker.CancellationPending)
                                        {
                                            if (totalIntersections > 0)
                                            {
                                                sql = " use Tiger2010StateFiles ";
                                                sql += " IF  EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + state + "_place10') ";
                                                sql += " update  " + state + "_place10 ";
                                                sql += " set num2000TractsIntersect = " + totalIntersections;
                                                sql += " WHERE placeFp10 = '" + id + "' ;";

                                                cmd = new SqlCommand(sql);
                                                qm = QueryManager;
                                                qm.AddParameters(cmd.Parameters);
                                                qm.ExecuteNonQuery(CommandType.Text, cmd.CommandText, 6000, true);
                                            }

                                        }
                                    }
                                    else
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Error occured in UpdatePlaceCT2000Counts: " + e.Message, e);
            }
        }

        public override void UpdatePlaceCT2010Counts()
        {
            string sql = "";
            try
            {

                if (States != null)
                {

                    foreach (string state in States)
                    {
                        if (!BackgroundWorker.CancellationPending)
                        {
                            TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "Update Tiger2010 Place Census Tract Counts: " + state + " - (" + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + ")");

                            sql += " use Tiger2010StateFiles ";
                            sql += " IF  EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + state + "_place10') ";
                            sql += " ALTER TABLE  " + state + "_place10 ";
                            sql += " ADD num2010TractsIntersect int null default 0; ";

                            SqlCommand cmd = new SqlCommand(sql);
                            IQueryManager qm = QueryManager;
                            qm.AddParameters(cmd.Parameters);
                            qm.ExecuteNonQuery(CommandType.Text, cmd.CommandText, 6000, true);

                            sql = "";
                            sql += " use Tiger2010StateFiles ";
                            sql += " IF  EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + state + "_place10') ";
                            sql += " Select placeFp10, shapeGeog from " + state + "_place10; ";

                            cmd = new SqlCommand(sql);
                            qm = QueryManager;
                            qm.AddParameters(cmd.Parameters);
                            DataTable dataTable = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, 6000, true);

                            if (dataTable != null && dataTable.Rows.Count > 0)
                            {
                                ProgressState.ProgressStateRecords.Total = dataTable.Rows.Count;
                                ProgressState.ProgressStateRecords.Completed = 0;
                                TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "got records : " + dataTable.Rows.Count);

                                int i = 1;

                                foreach (DataRow dataRow in dataTable.Rows)
                                {
                                    if (!BackgroundWorker.CancellationPending)
                                    {
                                        List<string> intersectingStates = new List<string>();
                                        int totalIntersections = 0;

                                        string id = (String)dataRow["placeFp10"];

                                        if (dataRow["shapeGeog"] != null && dataRow["shapeGeog"] != DBNull.Value)
                                        {
                                            SqlGeography sqlGeography = (SqlGeography)dataRow["shapeGeog"];

                                            if (sqlGeography != null)
                                            {

                                                ProgressState.ProgressStateRecords.Completed = i;
                                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.ProgressStateRecords.PercentCompleted), ProgressState);

                                                sql = "";
                                                sql += " use Tiger2010CensusTracts ";
                                                sql += " IF  EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + state + "_tract10') ";
                                                sql += " SELECT COUNT(uniqueId) ";
                                                sql += " FROM  " + state + "_tract10 ";
                                                sql += " WITH (INDEX (idx_geog))";
                                                sql += " WHERE shapeGeog.STIntersects(@shapeGeog) = 1 ;";

                                                cmd = new SqlCommand(sql);

                                                SqlParameter parameter = new SqlParameter("shapeGeog", sqlGeography);

                                                parameter.Direction = ParameterDirection.Input;
                                                parameter.ParameterName = "shapeGeog";
                                                parameter.SourceColumn = "shapeGeog";
                                                parameter.SqlDbType = SqlDbType.Udt;
                                                parameter.UdtTypeName = "GEOGRAPHY";
                                                parameter.SourceVersion = DataRowVersion.Current;

                                                cmd.Parameters.Add(parameter);


                                                qm = QueryManager;
                                                qm.AddParameters(cmd.Parameters);
                                                int intersections = qm.ExecuteScalarInt(CommandType.Text, cmd.CommandText, 6000, true);

                                                if (intersections >= 0)
                                                {
                                                    totalIntersections += intersections;

                                                    if (intersections > 0)
                                                    {
                                                        intersectingStates.Add(state);
                                                    }

                                                }
                                                else
                                                {
                                                    string here = "";
                                                }
                                            }



                                            i++;
                                        }

                                        if (!BackgroundWorker.CancellationPending)
                                        {
                                            if (totalIntersections > 0)
                                            {
                                                sql = " use Tiger2010StateFiles ";
                                                sql += " IF  EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + state + "_place10') ";
                                                sql += " update  " + state + "_place10 ";
                                                sql += " set num2010TractsIntersect = " + totalIntersections;
                                                sql += " WHERE placeFp10 = '" + id + "' ;";

                                                cmd = new SqlCommand(sql);
                                                qm = QueryManager;
                                                qm.AddParameters(cmd.Parameters);
                                                qm.ExecuteNonQuery(CommandType.Text, cmd.CommandText, 6000, true);
                                            }

                                        }
                                    }
                                    else
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Error occured in UpdatePlaceCT2000Counts: " + e.Message, e);
            }
        }


    }
}