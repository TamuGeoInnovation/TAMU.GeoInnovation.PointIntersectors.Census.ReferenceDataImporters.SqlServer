/*
 * Copyright ?2008 Daniel W. Goldberg
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

using System;
using System.Collections.Generic;
using System.Text;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Collections;
using System.Data;
using System.Diagnostics;
using System.IO;

using ICSharpCode.SharpZipLib.Zip;

using USC.GISResearchLab.Common.Utils.Directories;
using USC.GISResearchLab.Common.Utils.Strings;
using USC.GISResearchLab.Common.Utils.Files;
using USC.GISResearchLab.Common.Diagnostics.TraceEvents;
using USC.GISResearchLab.Common.Databases;
using USC.GISResearchLab.Common.Census;
using USC.GISResearchLab.Common.Utils.Databases;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.SchemaManagers;
using USC.GISResearchLab.Common.Census.Tiger2008.FileLayouts;
using USC.GISResearchLab.Common.Census.Tiger2008.FileLayouts.CountyFiles.Implementations;
using TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.ApplicationStates.Managers;
using USC.GISResearchLab.Common.Core.Databases.BulkCopys;
using USC.GISResearchLab.AddressProcessing.Core.Standardizing.StandardizedAddresses.Lines.LastLines;
using USC.GISResearchLab.Common.Census.Tiger2008.FileLayouts.AbstractClasses;
using USC.GISResearchLab.Common.Census.Tiger2008.FileLayouts.Interfaces;
using USC.GISResearchLab.Common.Census.Tiger2008.FileLayouts.StateFiles.Implementations;
using TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.FileLayouts.AbstractClasses.Tiger2000.StateFiles;
using USC.GISResearchLab.Common.Census.Tiger2008.FileLayouts.CountryFiles.AbstractClasses;
using USC.GISResearchLab.Common.Census.Tiger2008.FileLayouts.CountryFiles.Implementations;

namespace TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.Workers
{
    public abstract class SqlServerImporterWorker : AbstractImporterWorker
    {
        #region Properties



        #endregion

        public SqlServerImporterWorker()
            : base() { }

        public SqlServerImporterWorker(TraceSource traceSource, BackgroundWorker backgroundWorker, IQueryManager queryManager, ISchemaManager schemaManager)
            : base(traceSource, backgroundWorker, queryManager, schemaManager) { }


    }
}
