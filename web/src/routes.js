// Copyright 2018 Red Hat, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

import AutoholdPage from './pages/Autohold'
import AutoholdsPage from './pages/Autoholds'
import BuildPage from './pages/Build'
import BuildsPage from './pages/Builds'
import BuildsetPage from './pages/Buildset'
import BuildsetsPage from './pages/Buildsets'
import ChangeStatusPage from './pages/ChangeStatus'
import ComponentsPage from './pages/Components'
import ConfigErrorsPage from './pages/ConfigErrors'
import EnqueuePage from './pages/Enqueue'
import SystemEventsPage from './pages/SystemEvents'
import FreezeJobPage from './pages/FreezeJob'
import JobPage from './pages/Job'
import JobsPage from './pages/Jobs'
import ImagePage from './pages/Image'
import ImagesPage from './pages/Images'
import FlavorsPage from './pages/Flavors'
import LabelsPage from './pages/Labels'
import NodesetRequestsPage from './pages/NodesetRequests'
import NodesPage from './pages/Nodes'
import OpenApiPage from './pages/OpenApi'
import PipelineDetailsPage from './pages/PipelineDetails'
import PipelineOverviewPage from './pages/PipelineOverview'
import ProjectPage from './pages/Project'
import ProjectsPage from './pages/Projects'
import ProviderImagePage from './pages/ProviderImage'
import ProviderPage from './pages/Provider'
import ProvidersPage from './pages/Providers'
import RuntimePage from './pages/Runtime'
import SemaphorePage from './pages/Semaphore'
import SemaphoresPage from './pages/Semaphores'
import StreamPage from './pages/Stream'
import TenantsPage from './pages/Tenants'
import {
  isAuthorized,
} from './Misc'

// The Route object are created in the App component.
// Object with a title are created in the menu.
// Object with globalRoute are not tenant scoped.
// Remember to update the api getHomepageUrl subDir list for route with params
const routes = (info, user) => {
  const ret = [
    {
      title: 'Status',
      to: '/status',
      component: PipelineOverviewPage,
    },
    {
      title: 'Projects',
      to: '/projects',
      component: ProjectsPage
    },
    {
      title: 'Jobs',
      to: '/jobs',
      component: JobsPage
    },
    {
      title: 'Labels',
      to: '/labels',
      component: LabelsPage
    },
    {
      title: 'Nodes',
      to: '/nodes',
      component: NodesPage
    },
    {
      title: 'Autoholds',
      to: '/autoholds',
      component: AutoholdsPage
    },
    {
      title: 'Semaphores',
      to: '/semaphores',
      component: SemaphoresPage
    },
    {
      title: 'Builds',
      to: '/builds',
      component: BuildsPage
    },
    {
      title: 'Buildsets',
      to: '/buildsets',
      component: BuildsetsPage
    },
    {
      to: '/freeze-job',
      component: FreezeJobPage
    },
    {
      to: '/runtime',
      component: RuntimePage
    },
    {
      to: '/status/change/:changeId',
      component: ChangeStatusPage
    },
    {
      to: '/status/pipeline/:pipelineName',
      component: PipelineDetailsPage,
    },
    {
      to: '/stream/:buildId',
      component: StreamPage
    },
    {
      to: '/project/:projectName*',
      component: ProjectPage
    },
    {
      to: '/job/:jobName',
      component: JobPage
    },
    {
      to: '/build/:buildId/:activeTab?/:file*',
      component: BuildPage,
    },
    {
      to: '/buildset/:buildsetId',
      component: BuildsetPage
    },
    {
      to: '/autohold/:requestId',
      component: AutoholdPage
    },
    {
      to: '/semaphore/:semaphoreName',
      component: SemaphorePage
    },
    {
      to: '/config-errors',
      component: ConfigErrorsPage,
    },
    {
      to: '/system-events',
      component: SystemEventsPage,
    },
    {
      to: '/tenants',
      component: TenantsPage,
      globalRoute: true
    },
    {
      to: '/openapi',
      component: OpenApiPage,
      noTenantPrefix: true,
    },
    {
      to: '/components',
      component: ComponentsPage,
      noTenantPrefix: true,
    },
    // auth_callback is handled in App.jsx
  ]
  if (info && info.niz) {
    ret.push(
      {
        title: 'Providers',
        to: '/providers',
        component: ProvidersPage,
      }
    )
    ret.push(
      {
        to: '/provider/:providerName',
        component: ProviderPage,
      }
    )
    ret.push(
      {
        to: '/provider/:providerName/image/:imageName',
        component: ProviderImagePage,
      }
    )
    ret.push(
      {
        title: 'Images',
        to: '/images',
        component: ImagesPage,
      }
    )
    ret.push(
      {
        to: '/image/:imageName',
        component: ImagePage,
      }
    )
    ret.push(
      {
        title: 'Flavors',
        to: '/flavors',
        component: FlavorsPage,
      }
    )
    ret.splice(5, 0,
      {
        title: 'Requests',
        to: '/nodeset-requests',
        component: NodesetRequestsPage,
      }
    )
  }
  if (user && isAuthorized(user, 'enqueue')) {
    ret.push(
      {
        title: 'Enqueue',
        to: '/enqueue',
        component: EnqueuePage
      },
    )
  }
  return ret
}

export { routes }
