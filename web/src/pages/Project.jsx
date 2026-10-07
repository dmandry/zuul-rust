// Copyright 2018 Red Hat, Inc
// Copyright 2022 Acme Gating, LLC
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

import React, { useEffect, useCallback } from 'react'
import { connect } from 'react-redux'
import PropTypes from 'prop-types'
import {
  PageSection,
  PageSectionVariants,
  Text,
  TextContent,
  DescriptionList,
  DescriptionListGroup,
  DescriptionListDescription,
  DescriptionListTerm,
} from '@patternfly/react-core'
import Project from '../containers/project/Project'
import JobGraph from '../containers/jobgraph/JobGraph'
import { fetchProjectIfNeeded } from '../actions/project'
import { Fetchable } from '../containers/Fetching'
import SSHKey from '../containers/SSHKey'


function ProjectPage(props) {
  const { tenant, fetchProjectIfNeeded, remoteData } = props
  const { projectName } = props.match.params
  const tenantProjects = remoteData.projects[tenant.name]

  const updateData = useCallback((force) => {
    if (tenant.name) {
      fetchProjectIfNeeded(tenant, projectName, force)
    }
  }, [tenant, projectName, fetchProjectIfNeeded])

  useEffect(() => {
    document.title = 'Zuul Project | ' + projectName
    updateData()
  }, [tenant, projectName, updateData])

  return (
    <>
      <PageSection variant={props.preferences.darkMode ? PageSectionVariants.dark : PageSectionVariants.light}>
        <TextContent>
          <Text component="h2">Project {projectName}</Text>
          <Fetchable
            isFetching={remoteData.isFetching}
            fetchCallback={updateData}
          />
        </TextContent>
        {tenantProjects && tenantProjects[projectName] &&
         <>
           <Project project={tenantProjects[projectName]} />
         <DescriptionList isHorizontal
                          style={{'--pf-c-description-list--RowGap': '0.5rem'}}
                          className='pf-u-m-xl'>
           <DescriptionListGroup key={1}>
             <DescriptionListTerm>
               Project SSH key
             </DescriptionListTerm>
             <DescriptionListDescription>
               <SSHKey projectName={projectName}/>
             </DescriptionListDescription>
           </DescriptionListGroup>
           <DescriptionListGroup key={1}>
             <DescriptionListTerm>
               Tenant SSH key
             </DescriptionListTerm>
             <DescriptionListDescription>
               <SSHKey/>
             </DescriptionListDescription>
           </DescriptionListGroup>
        </DescriptionList>
           <JobGraph project={tenantProjects[projectName]} />
         </>
        }
      </PageSection>
    </>
  )
}

ProjectPage.propTypes = {
  match: PropTypes.object.isRequired,
  tenant: PropTypes.object,
  remoteData: PropTypes.object,
  fetchProjectIfNeeded: PropTypes.func,
  preferences: PropTypes.object,
}

function mapStateToProps(state) {
  return {
    tenant: state.tenant,
    remoteData: state.project,
    preferences: state.preferences,
  }
}

const mapDispatchToProps = {
  fetchProjectIfNeeded,
}

export default connect(mapStateToProps, mapDispatchToProps)(ProjectPage)
