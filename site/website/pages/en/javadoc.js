const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');

const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();
const versions = require(`${CWD}/all-versions.json`);


class JavaDoc extends React.Component {
  render() {
    const {config: siteConfig} = this.props;
    const latestVersion = versions[0];
  
    return (
        <div className="docMainWrapper wrapper">
        <Container className="container mainContainer postContainer blogContainer ">
          <div className="post">
            <header className="postHeader">
              <h1>{siteConfig.title} API docs</h1>
            </header>
            <p>Hazelcast Jet contains extensive JavaDoc and should be
            consulted as the primary source of information regarding specific
            API details.
            </p>
            <h3 id="latest">Current version (Stable)</h3>
            <table className="versions">
              <tbody>
                <tr>
                  <th>{latestVersion}</th>
                  <td>
                    <a
                      href={`/javadoc/${latestVersion}`}>
                      JavaDoc
                    </a>
                  </td>
                </tr>
              </tbody>
            </table>
            <h3 id="archive">Past Versions</h3>
            <p>Here you can find previous versions of the API docs.</p>
            <table className="versions">
              <tbody>
                {versions.map(
                  version =>
                    version !== latestVersion && (
                      <tr key={version}>
                        <th>{version}</th>
                        <td>
                          <a
                            href={`/javadoc/${version}`}>
                            JavaDoc
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
}

module.exports = JavaDoc;
