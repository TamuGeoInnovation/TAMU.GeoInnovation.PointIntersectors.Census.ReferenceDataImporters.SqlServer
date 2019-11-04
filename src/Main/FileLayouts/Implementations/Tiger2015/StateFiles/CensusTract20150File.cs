using TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.FileLayouts.AbstractClasses.Tiger2015.StateFiles;

namespace TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.SqlServer.FileLayouts.Implementations.Tiger2015.StateFiles
{
    public class CensusTract2015File : AbstractTiger2015ShapefileStateFileLayout
    {



        public CensusTract2015File(string stateName)
            : base(stateName)
        {

            ExcludeColumns = new string[]
            {
                "uniqueId"
            };


            FileName = "tract10.zip";

            SQLCreateTable += "CREATE TABLE [" + OutputTableName + "] (";
            SQLCreateTable += "uniqueId int identity (1,1) not null ,";
            SQLCreateTable += "stateFp10 varchar(2) DEFAULT NULL,";
            SQLCreateTable += "countyFp10 varchar(3) DEFAULT NULL,";
            SQLCreateTable += "TractCe10 varchar(6) DEFAULT NULL,";
            SQLCreateTable += "GEOID10 varchar(11) DEFAULT NULL,";
            SQLCreateTable += "Name10 varchar(7) DEFAULT NULL,";
            SQLCreateTable += "NameLsad10 varchar(20) DEFAULT NULL,";
            SQLCreateTable += "Mtfcc10 varchar(5) DEFAULT NULL,";
            SQLCreateTable += "FuncStat10 varchar(1) DEFAULT NULL,";
            SQLCreateTable += "INTPTLAT10  varchar(11) DEFAULT NULL,";
            SQLCreateTable += "INTPTLON10 varchar(12) DEFAULT NULL,";
            SQLCreateTable += "shapeType varchar(55), ";
            SQLCreateTable += "shapeGeog geography,";
            SQLCreateTable += "shapeGeom geometry,";
            SQLCreateTable += "PRIMARY KEY  (uniqueId)";
            SQLCreateTable += ");";

        }
    }
}
