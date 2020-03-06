/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');
const CompLibrary = require('../../core/CompLibrary');
const Container = CompLibrary.Container;

const CWD = process.cwd();

const versions = require(`${CWD}/all-versions.json`);

function Downloads(props) {
  const {config: siteConfig} = props;
  const latestVersion = versions[0];
  const repoUrl = `https://github.com/${siteConfig.organizationName}/${siteConfig.projectName}`;
  return (
    <div className="docMainWrapper wrapper">
      <Container className="mainContainer versionsContainer">
        <div className="post">
          <header className="postHeader">
            <h1>{siteConfig.title} Downloads</h1>
          </header>
          <h3 id="latest">Current version (Stable)</h3>
          <table className="versions">
            <tbody>
              <tr>
                <th>{latestVersion}</th>
                <td>
                <a href={`${repoUrl}/releases/download/v${latestVersion}/hazelcast-jet-${latestVersion}.zip`}>
                        Download
                </a>
                </td>
                <td>
                  <a href={`${repoUrl}/releases/tag/v${latestVersion}`}>
                        Release Notes
                  </a>
                </td>
              </tr>
            </tbody>
          </table>
          <p>Hazelcast Jet artifacts can also be retrieved using the following Maven coordinates:</p>
          
          <pre><code class="language-groovy css hljs">
              groupId: <span class="hljs-string">com.hazelcast.jet</span><br/>
              artifactId: <span class="hljs-string">hazelcast-jet</span><br/>
              version: <span class="hljs-string">{latestVersion}</span>
         </code></pre>
         <p>For the full list of modules, please see <a href="https://search.maven.org/search?q=g:com.hazelcast.jet">Maven Central</a>.</p>
          <h3 id="archive">Past Versions</h3>
          <p>Here you can find previous versions of Hazelcast Jet.</p>
          <table className="versions">
            <tbody>
              {versions.map(
                version =>
                  version !== latestVersion && (
                    <tr key={version}>
                      <th>{version}</th>
                      <td>
                      <a href={`${repoUrl}/releases/download/v${version}/hazelcast-jet-${version}.zip`}>
                        Download
                      </a>
                      </td>
                      <td>
                        <a href={`${repoUrl}/releases/tag/v${version}`}>
                          Release Notes
                        </a>
                      </td>
                    </tr>
                  ),
              )}
            </tbody>
          </table>
        </div>
      </Container>
    </div>
  );
}

module.exports = Downloads;
