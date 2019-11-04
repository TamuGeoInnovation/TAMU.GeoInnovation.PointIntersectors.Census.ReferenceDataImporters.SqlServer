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

using System.ComponentModel;
using System.Diagnostics;
using TAMU.GeoInnovation.Applications.Census.ReferenceDataImporter.Workers;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Databases.SchemaManagers;

namespace TAMU.GeoInnovation.PointIntersectors.Census.ReferenceDataImporters.SqlServer
{
    public class SqlServerStateLevelImporterWorker : AbstractStateLevelImporterWorker
    {
        #region Properties

        #endregion

        public SqlServerStateLevelImporterWorker() : base() { }

        public SqlServerStateLevelImporterWorker(TraceSource traceSource, BackgroundWorker backgroundWorker, IQueryManager outputDataQueryManager, ISchemaManager schemaManager)
            : base(traceSource, backgroundWorker, outputDataQueryManager, schemaManager) { }



    }
}
