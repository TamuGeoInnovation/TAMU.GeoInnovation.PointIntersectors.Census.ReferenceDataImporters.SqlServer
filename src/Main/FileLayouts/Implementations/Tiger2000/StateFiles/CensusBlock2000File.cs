using TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.FileLayouts.AbstractClasses.Tiger2010.StateFiles;

namespace TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.SqlServer.FileLayouts.Implementations.Tiger2000.StateFiles
{
    public class CensusBlock2000File : AbstractTiger2010ShapefileStateFileLayout
    {

        public CensusBlock2000File(string stateName)
            : base(stateName)
        {

            FileName = "tabblock00.zip";

            SQLCreateTable += "CREATE TABLE [" + OutputTableName + "] (";
            SQLCreateTable += "stateFp00 varchar(2) DEFAULT NULL,";
            SQLCreateTable += "countyFp00 varchar(3) DEFAULT NULL,";
            SQLCreateTable += "TractCe00 varchar(6) DEFAULT NULL,";
            SQLCreateTable += "BlockCe00 varchar(4) DEFAULT NULL,";
            SQLCreateTable += "blkIdFp00 varchar(15) DEFAULT NULL,";
            SQLCreateTable += "Name00 varchar(10) DEFAULT NULL,";
            SQLCreateTable += "Mtfcc00 varchar(5) DEFAULT NULL,";
            SQLCreateTable += "UR00 varchar(1) DEFAULT NULL,";
            SQLCreateTable += "UAce00 varchar(5) DEFAULT NULL,";
            SQLCreateTable += "FuncStat00 varchar(1) DEFAULT NULL,";
            SQLCreateTable += "shapeType varchar(55), ";
            SQLCreateTable += "shapeGeog geography,";
            SQLCreateTable += "shapeGeom geometry,";
            SQLCreateTable += "PRIMARY KEY  (blkIdFp00)";
            SQLCreateTable += ");";

        }
    }
}
