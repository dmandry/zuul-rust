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

import React, { useEffect, useState, useMemo } from 'react'
import { useSelector, useDispatch } from 'react-redux'
import {
  useHistory,
  useLocation,
  useRouteMatch,
} from 'react-router-dom'
import queryString from 'query-string'
import {
  Button,
  EmptyState,
  EmptyStateVariant,
  EmptyStateIcon,
  PageSection,
  PageSectionVariants,
  Tab,
  Tabs,
  TabTitleIcon,
  TabTitleText,
  Title,
} from '@patternfly/react-core'
import {
  ArrowUpIcon,
  BuildIcon,
  FileArchiveIcon,
  FileCodeIcon,
  TerminalIcon,
  PollIcon,
  ExclamationIcon,
} from '@patternfly/react-icons'

import { fetchBuildAllInfo } from '../actions/build'
import { EmptyPage } from '../containers/Errors'
import { Fetching } from '../containers/Fetching'
import ArtifactList from '../containers/build/Artifact'
import Build from '../containers/build/Build'
import BuildOutput from '../containers/build/BuildOutput'
import Console from '../containers/build/Console'
import Manifest from '../containers/build/Manifest'
import LogFile from '../containers/logfile/LogFile'

function handleTabClick(tabIndex, history, tenant, build) {
  // Usually tabs should only be used to display content in-page and not link
  // to other pages:
  // "Tabs are used to present a set on tabs for organizing content on a
  // .page. It must always be used together with a tab content component."
  // https://www.patternfly.org/v4/documentation/react/components/tabs
  // But as want to be able to reach every tab's content via a dedicated URL
  // while having the look and feel of tabs, we could hijack this onClick
  // handler to do the link/routing stuff.

  switch (tabIndex) {
    case 'artifacts':
      history.push(`${tenant.linkPrefix}/build/${build.uuid}/artifacts`)
      break
    case 'logs':
      history.push(`${tenant.linkPrefix}/build/${build.uuid}/logs`)
      break
    case 'console':
      history.push(`${tenant.linkPrefix}/build/${build.uuid}/console`)
      break
    default:
      // task summary
      history.push(`${tenant.linkPrefix}/build/${build.uuid}`)
  }
}

function scrollToTop() {
  window.scrollTo(0,0)
  document.activeElement.blur()
}

function BuildPageComponent() {
  const dispatch = useDispatch()
  const location = useLocation();
  const history = useHistory();
  const match = useRouteMatch();
  const activeTab = match.params.activeTab
        ? (
          match.params.activeTab === "log"
            ? "logs"
            : match.params.activeTab
        )
        : "results"
  const buildId = match.params.buildId
  const tenant = useSelector((state) => state.tenant)
  const preferences = useSelector((state) => state.preferences)
  const stateBuild = useSelector((state) => state.build)
  const stateLogfile = useSelector((state) => state.logfile)
  const stateLogfiles = stateLogfile.files
  const logfileName = match.params.file
  const logfile =
    buildId in stateLogfiles
      ? stateLogfiles[buildId][logfileName]
      : undefined
  const output = stateBuild.outputs[buildId]
  const hosts = stateBuild.hosts[buildId]
  const errorIds = stateBuild.errorIds[buildId]
  const manifest = stateBuild.manifests[buildId]
  const build = stateBuild.builds[buildId]
  const isFetching = stateBuild.isFetching
  const isFetchingManifest =  stateBuild.isFetchingManifest
  const isFetchingOutput =  stateBuild.isFetchingOutput
  const isFetchingLogfile = stateLogfile.isFetching
  const severity = parseInt(queryString.parse(location.search).severity)
  const hash = useMemo(()=>location.hash.substring(1).split('/'),
                       [location.hash])

  const [topOfPageVisible, setTopOfPageVisible] = useState(true)

  function onScroll() {
    setTopOfPageVisible(window.scrollY === 0)
  }

  useEffect(() => {
    document.title = 'Zuul Build'
    window.addEventListener('scroll', onScroll)
  }, [])

  useEffect(() => {
    // The related fetchBuild...() methods won't do anything if the data is
    // already available in the local state, so just call them.
    dispatch(fetchBuildAllInfo(
      tenant,
      match.params.buildId,
      match.params.file
    ))
  }, [tenant, dispatch, match.params.buildId, match.params.file])

  function handleBreadcrumbItemClick() {
    // Simply link back to the logs tab without an active logfile
    handleTabClick('logs', history, tenant, build)
  }

  // In case the build is not available yet (before the fetching started) or
  // is currently fetching.
  if (build === undefined || isFetching) {
    return <Fetching />
  }

  // The build is null, meaning it couldn't be found.
  if (!build) {
    return (
      <EmptyPage
        title="This build does not exist"
        icon={BuildIcon}
        linkTarget={`${tenant.linkPrefix}/builds`}
        linkText="Show all builds"
      />
    )
  }

  const resultsTabContent =
    hosts === undefined || isFetchingOutput ? (
      <Fetching />
    ) : hosts ? (
      <BuildOutput output={hosts} />
    ) : build.error_detail ? (
      <>
      <EmptyState variant={EmptyStateVariant.small}>
        <EmptyStateIcon icon={ExclamationIcon} />
      </EmptyState>
        <p><b>Error:</b> {build.error_detail}</p>
      </>
    ) : (
      <EmptyState variant={EmptyStateVariant.small}>
        <EmptyStateIcon icon={PollIcon} />
        <Title headingLevel="h4" size="lg">
          This build does not provide any results
        </Title>
      </EmptyState>
    )

  const artifactsTabContent = build.artifacts.length ? (
    <ArtifactList artifacts={build.artifacts} />
  ) : (
    <EmptyState variant={EmptyStateVariant.small}>
      <EmptyStateIcon icon={FileArchiveIcon} />
      <Title headingLevel="h4" size="lg">
        This build does not provide any artifacts
      </Title>
    </EmptyState>
  )

  let logsTabContent = null
  if (manifest === undefined || isFetchingManifest) {
    logsTabContent = <Fetching />
  } else if (logfileName) {
    logsTabContent = (
      <LogFile
        logfileContent={logfile}
        logfileName={logfileName}
        isFetching={isFetchingLogfile}
        // We let the LogFile component itself handle the severity default
        // value in case it's not set via the URL.
        severity={severity ? severity : undefined}
        handleBreadcrumbItemClick={handleBreadcrumbItemClick}
        location={location}
        history={history}
      />
    )
  // Do not render the Manifest component if we don't have a log_url (this
  // can happen for CANCELLED builds) since the log file paths are
  // constructed from the build.log_url. Instead show the EmptyState.
  } else if (manifest && build.log_url) {
    logsTabContent = <Manifest tenant={tenant}
                               build={build}
                               manifest={manifest} />
  } else {
    logsTabContent = (
      <EmptyState variant={EmptyStateVariant.small}>
        <EmptyStateIcon icon={FileCodeIcon} />
        <Title headingLevel="h4" size="lg">
          This build does not provide any logs
        </Title>
      </EmptyState>
    )
  }

  const consoleTabContent =
    output === undefined || isFetchingOutput ? (
      <Fetching />
    ) : output ? (
      <Console
        errorIds={errorIds}
        output={output}
        displayPath={hash}
      />
    ) : (
      <EmptyState variant={EmptyStateVariant.small}>
        <EmptyStateIcon icon={TerminalIcon} />
        <Title headingLevel="h4" size="lg">
          This build does not provide any console information
        </Title>
      </EmptyState>
    )

  return (
    <>
      <PageSection variant={preferences.darkMode ? PageSectionVariants.dark : PageSectionVariants.light}>
        <Build build={build} manifest={manifest}
               active={activeTab} hash={hash} />
      </PageSection>
      <PageSection variant={preferences.darkMode ? PageSectionVariants.dark : PageSectionVariants.light}>
        <Tabs
          isFilled
          activeKey={activeTab}
          onSelect={(event, tabIndex) =>
            handleTabClick(tabIndex, history, tenant, build)}
        >
          <Tab
            eventKey="results"
            title={
              <>
                <TabTitleIcon>
                  <PollIcon />
                </TabTitleIcon>
                <TabTitleText>Task Summary</TabTitleText>
              </>
            }
          >
            {resultsTabContent}
          </Tab>
          <Tab
            eventKey="artifacts"
            title={
              <>
                <TabTitleIcon>
                  <FileArchiveIcon />
                </TabTitleIcon>
                <TabTitleText>Artifacts</TabTitleText>
              </>
            }
          >
            {artifactsTabContent}
          </Tab>
          <Tab
            eventKey="logs"
            title={
              <>
                <TabTitleIcon>
                  <FileCodeIcon />
                </TabTitleIcon>
                <TabTitleText>Logs</TabTitleText>
              </>
            }
          >
            {logsTabContent}
          </Tab>
          <Tab
            eventKey="console"
            title={
              <>
                <TabTitleIcon>
                  <TerminalIcon />
                </TabTitleIcon>
                <TabTitleText>Console</TabTitleText>
              </>
            }
          >
            {consoleTabContent}
          </Tab>
        </Tabs>
      </PageSection>
      {!topOfPageVisible && (
        <PageSection variant={preferences.darkMode ? PageSectionVariants.dark : PageSectionVariants.light}>
          <Button onClick={scrollToTop} variant="primary" style={{position: 'fixed', bottom: 20, right: 20, zIndex: 1}}>
            Go to top of page <ArrowUpIcon/>
          </Button>
        </PageSection>
      )}
    </>
  )
}

export default BuildPageComponent
