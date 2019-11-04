

using TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.FileLayouts.AbstractClasses.Tiger2015.CountryFiles;

namespace TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.SqlServer.FileLayouts.Implementations.Tiger2015.CountryFiles
{
    public class States2015File : AbstractTiger2015ShapefileCountryFileLayout
    {

        public States2015File(string stateName)
            : base(stateName)
        {

            ExcludeColumns = new string[]
            {
                "uniqueId"
            };

            FileName = "state10.zip";

            SQLCreateTable += "CREATE TABLE [" + OutputTableName + "] (";
            SQLCreateTable += "uniqueId int IDENTITY(1,1) NOT NULL,";
            SQLCreateTable += "region varchar(2) DEFAULT NULL,";
            SQLCreateTable += "division varchar(2) DEFAULT NULL,";
            SQLCreateTable += "stateFp varchar(2) DEFAULT NULL,";
            SQLCreateTable += "stateNs varchar(8) DEFAULT NULL,";
            SQLCreateTable += "geoId varchar(7) DEFAULT NULL,";
            SQLCreateTable += "stUsPs varchar(2) DEFAULT NULL,";
            SQLCreateTable += "name varchar(100) DEFAULT NULL,";
            SQLCreateTable += "lsad varchar(2) DEFAULT NULL,";
            SQLCreateTable += "Mtfcc varchar(5) DEFAULT NULL,";
            SQLCreateTable += "FuncStat varchar(1) DEFAULT NULL,";
            SQLCreateTable += "aLand varchar(14) DEFAULT NULL,";
            SQLCreateTable += "aWater varchar(14) DEFAULT NULL,";
            SQLCreateTable += "intPtLat varchar(11) DEFAULT NULL,";
            SQLCreateTable += "intPtLon varchar(12) DEFAULT NULL,";
            SQLCreateTable += "shapeType varchar(55), ";
            SQLCreateTable += "shapeGeog geography,";
            SQLCreateTable += "shapeGeom geometry,";
            SQLCreateTable += "PRIMARY KEY  (uniqueId)";
            SQLCreateTable += ");";

            SQLCreateTableIndexes += " CREATE NONCLUSTERED INDEX [IDX_" + OutputTableName + "Name] ON [dbo].[" + OutputTableName + "] (Name) WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY];";
            SQLCreateTableIndexes += " CREATE NONCLUSTERED INDEX [IDX_" + OutputTableName + "stUsPs10] ON [dbo].[" + OutputTableName + "] (stUsPs10) WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY];";

        }
    }
}
