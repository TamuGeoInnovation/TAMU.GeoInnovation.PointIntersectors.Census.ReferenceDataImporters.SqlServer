using TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.FileLayouts.AbstractClasses.Tiger2010.CountryFiles;

namespace TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.SqlServer.FileLayouts.Implementations.Tiger2000.CountryFiles
{
    public class Zcta52000File : AbstractTiger2010ShapefileCountryFileLayout
    {

        public Zcta52000File(string stateName)
            : base(stateName)
        {

            FileName = "zcta500.zip";

            ShouldIncludeArea = true;
            ShouldIncludeCentroid = true;

            SQLCreateTable += "CREATE TABLE [" + OutputTableName + "] (";
            SQLCreateTable += "zcta5ce00 varchar(5) DEFAULT NULL,";
            SQLCreateTable += "classFp00 varchar(2) DEFAULT NULL,";
            SQLCreateTable += "Mtfcc00 varchar(5) DEFAULT NULL,";
            SQLCreateTable += "FuncStat00 varchar(1) DEFAULT NULL,";
            SQLCreateTable += "shapeType varchar(55), ";
            SQLCreateTable += "shapeGeog geography,";
            SQLCreateTable += "shapeGeom geometry,";
            SQLCreateTable += "shapeArea varchar(55) ,";
            SQLCreateTable += "centroidX varchar(55) ,";
            SQLCreateTable += "centroidY varchar(55) ,";
            SQLCreateTable += "PRIMARY KEY  (zcta5ce00)";
            SQLCreateTable += ");";

            SQLCreateTableIndexes += " CREATE NONCLUSTERED INDEX [IDX_" + OutputTableName + "zcta5ce00] ON [dbo].[" + OutputTableName + "] (zcta5ce00) WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY];";

        }
    }
}
