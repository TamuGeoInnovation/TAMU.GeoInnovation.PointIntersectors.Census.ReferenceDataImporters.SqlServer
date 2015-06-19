using TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.FileLayouts.AbstractClasses.Tiger2010.CountryFiles;

namespace TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.SqlServer.FileLayouts.Implementations.Tiger2000.CountryFiles
{
    public class States2000File : AbstractTiger2010ShapefileCountryFileLayout
    {

        public States2000File(string stateName)
            : base(stateName)
        {

            FileName = "state00.zip";

            SQLCreateTable += "CREATE TABLE [" + OutputTableName + "] (";
            SQLCreateTable += "stateFp00 varchar(2) DEFAULT NULL,";
            SQLCreateTable += "stUsPs00 varchar(2) DEFAULT NULL,";
            SQLCreateTable += "name00 varchar(100) DEFAULT NULL,";
            SQLCreateTable += "lsad00 varchar(2) DEFAULT NULL,";
            SQLCreateTable += "Mtfcc00 varchar(5) DEFAULT NULL,";
            SQLCreateTable += "ur00 varchar(1) DEFAULT NULL,";
            SQLCreateTable += "FuncStat00 varchar(1) DEFAULT NULL,";
            SQLCreateTable += "shapeType varchar(55), ";
            SQLCreateTable += "shapeGeog geography,";
            SQLCreateTable += "shapeGeom geometry,";
            SQLCreateTable += "PRIMARY KEY  (name00)";
            SQLCreateTable += ");";

            SQLCreateTableIndexes += " CREATE NONCLUSTERED INDEX [IDX_" + OutputTableName + "Name00] ON [dbo].[" + OutputTableName + "] (Name00) WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY];";
            SQLCreateTableIndexes += " CREATE NONCLUSTERED INDEX [IDX_" + OutputTableName + "stUsPs00] ON [dbo].[" + OutputTableName + "] (stUsPs00) WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY];";

        }
    }
}
