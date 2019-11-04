using TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.FileLayouts.AbstractClasses.Tiger2010.StateFiles;

namespace TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.SqlServer.FileLayouts.Implementations.Tiger2000.StateFiles
{
    public class Place2000File : AbstractTiger2010ShapefileStateFileLayout
    {

        public Place2000File(string stateName)
            : base(stateName)
        {

            FileName = "place00.zip";

            HasSoundexColumns = true;
            HasSoundexDMColumns = true;
            SoundexColumns = new string[] { "Name00" };
            SoundexDMColumns = new string[] { "Name00" };

            //ExcludeColumns = new string[] { "Name00_Soundex", "Name00_SoundexDM" };

            SQLCreateTable += "CREATE TABLE [" + OutputTableName + "] (";
            SQLCreateTable += "stateFp00 varchar(2) DEFAULT NULL,";
            SQLCreateTable += "placeFp00 varchar(5) DEFAULT NULL,";
            SQLCreateTable += "plcIdFp00 varchar(7) DEFAULT NULL,";
            SQLCreateTable += "Name00 varchar(100) DEFAULT NULL,";
            SQLCreateTable += "Name00_Soundex varchar(4) DEFAULT NULL,";
            SQLCreateTable += "Name00_SoundexDM varchar(6) DEFAULT NULL,";
            SQLCreateTable += "NameLsad00 varchar(100) DEFAULT NULL,";
            SQLCreateTable += "lsad00 varchar(2) DEFAULT NULL,";
            SQLCreateTable += "classFp00 varchar(2) DEFAULT NULL,";
            SQLCreateTable += "cpi00 varchar(1) DEFAULT NULL,";
            SQLCreateTable += "pciCbsa00 varchar(1) DEFAULT NULL,";
            SQLCreateTable += "pciNeCta00 varchar(1) DEFAULT NULL,";
            SQLCreateTable += "Mtfcc00 varchar(5) DEFAULT NULL,";
            SQLCreateTable += "ur00 varchar(1) DEFAULT NULL,";
            SQLCreateTable += "FuncStat00 varchar(1) DEFAULT NULL,";
            SQLCreateTable += "shapeType varchar(55), ";
            SQLCreateTable += "shapeGeog geography,";
            SQLCreateTable += "shapeGeom geometry,";
            SQLCreateTable += "PRIMARY KEY  (placeFp00)";
            SQLCreateTable += ");";

            SQLCreateTableIndexes += " CREATE NONCLUSTERED INDEX [IDX_" + OutputTableName + "Name00] ON [dbo].[" + OutputTableName + "] (Name00) WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY];";
            SQLCreateTableIndexes += " CREATE NONCLUSTERED INDEX [IDX_" + OutputTableName + "Name00_Soundex] ON [dbo].[" + OutputTableName + "] (Name00_Soundex) WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY];";
            SQLCreateTableIndexes += " CREATE NONCLUSTERED INDEX [IDX_" + OutputTableName + "Name00_SoundexDM] ON [dbo].[" + OutputTableName + "] (Name00_SoundexDM) WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY];";

        }
    }
}
