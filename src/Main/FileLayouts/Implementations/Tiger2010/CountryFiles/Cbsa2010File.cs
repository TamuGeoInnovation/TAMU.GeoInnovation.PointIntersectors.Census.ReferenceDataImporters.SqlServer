using TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.FileLayouts.AbstractClasses.Tiger2010.CountryFiles;

namespace TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.SqlServer.FileLayouts.Implementations.Tiger2010.CountryFiles
{
    public class Cbsa2010File : AbstractTiger2010ShapefileCountryFileLayout
    {

        public Cbsa2010File(string stateName)
            : base(stateName)
        {

            FileName = "cbsa10.zip";

            SQLCreateTable += "CREATE TABLE [" + OutputTableName + "] (";
            SQLCreateTable += "csaFp10 varchar(3) DEFAULT NULL,";
            SQLCreateTable += "cbsaFp10 varchar(5) DEFAULT NULL,";
            SQLCreateTable += "geoId10 varchar(10) DEFAULT NULL,";
            SQLCreateTable += "Name10 varchar(100) DEFAULT NULL,";
            SQLCreateTable += "NameLsad10 varchar(100) DEFAULT NULL,";
            SQLCreateTable += "Lsad10 varchar(2) DEFAULT NULL,";
            SQLCreateTable += "memi10 varchar(1) DEFAULT NULL,";
            SQLCreateTable += "Mtfcc10 varchar(5) DEFAULT NULL,";
            SQLCreateTable += "aLand10 varchar(14) DEFAULT NULL,";
            SQLCreateTable += "aWater10 varchar(14) DEFAULT NULL,";
            SQLCreateTable += "intPtLat10 varchar(11) DEFAULT NULL,";
            SQLCreateTable += "intPtLon10 varchar(12) DEFAULT NULL,";
            SQLCreateTable += "shapeType varchar(55), ";
            SQLCreateTable += "shapeGeog geography,";
            SQLCreateTable += "shapeGeom geometry,";
            SQLCreateTable += "PRIMARY KEY  (geoId10)";
            SQLCreateTable += ");";

            SQLCreateTableIndexes += " CREATE NONCLUSTERED INDEX [IDX_" + OutputTableName + "geoId10] ON [dbo].[" + OutputTableName + "] (geoId10) WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY];";
        }
    }
}
