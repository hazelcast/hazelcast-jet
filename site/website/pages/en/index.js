/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');

const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

class HomeSplash extends React.Component {
  render() {
    const {siteConfig, language = ''} = this.props;
    const {baseUrl, docsUrl} = siteConfig;
    const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`;
    const langPart = `${language ? `${language}/` : ''}`;
    const docUrl = doc => `${baseUrl}${docsPart}${langPart}${doc}`;

    const SplashContainer = props => (
      <div className="homeContainer">
        <div className="homeSplashFade">
          <div className="wrapper homeWrapper">{props.children}</div>
        </div>
      </div>
    );

    const Logo = props => (
      <div className="projectLogo">
        <img src={props.img_src} alt="Project Logo" />
      </div>
    );

    const ProjectTitle = props => (
      <h2 className="projectTitle">
        {props.title}
        <small>{props.tagline}</small>
      </h2>
    );

    const PromoSection = props => (
      <div className="section promoSection">
        <div className="promoRow">
          <div className="pluginRowBlock">{props.children}</div>
        </div>
      </div>
    );

    const Button = props => (
      <div className="pluginWrapper buttonWrapper">
        <a className="button" href={props.href} target={props.target}>
          {props.children}
        </a>
      </div>
    );

    return (
      <SplashContainer>
        <Logo img_src={`${baseUrl}img/logo-big.png`} />
        <div className="inner">
          <ProjectTitle tagline={siteConfig.tagline} title={siteConfig.title} />
          <PromoSection>
            <Button href={docUrl('get-started')}>Getting Started</Button>
            <Button href="https://docs.hazelcast.org/docs/jet/latest-dev/manual/">Reference Manual</Button>
            <Button href="https://github.com/hazelcast/hazelcast-jet">Github</Button>
          </PromoSection>
        </div>
      </SplashContainer>
    );
  }
}

class Index extends React.Component {
  render() {
    const {config: siteConfig, language = ''} = this.props;
    const {baseUrl} = siteConfig;

    const Block = props => (
      <Container
        padding={['bottom', 'top']}
        id={props.id}
        background={props.background}>
        <GridBlock
          align="center"
          contents={props.children}
          layout={props.layout}
        />
      </Container>
    );

    const FeatureCallout = () => (
      <div
        className="productShowcaseSection paddingBottom"
        style={{textAlign: 'center'}}>
        <h2>Distributed Computing, Simplified.</h2>
      </div>
    );


    const Features = () => (
      <Block layout="fourColumn">
        {[
          {
            title: 'Simple',
            image: `${baseUrl}img/undraw_relaxation.svg`,
            imageAlign: 'top',
            content: 'Jet is simple to set up. The nodes automatically discover each other to form a cluster. ' +
            'You can do the same locally, even on the same machine (your laptop, for example). This is great for quick testing, ' +
            'fast deployment, and easier ongoing maintenance.',
          },
          {
            title: 'Runs everywhere',
            image: `${baseUrl}img/undraw_superhero.svg`,
            imageAlign: 'top',
            content: 'Jet is delivered as a single JAR without dependencies that requires Java 8 to run with full ' +
          'functionality. It’s lightweight to run on small devices, and it’s cloud-native with Docker images and' +
          ' Kubernetes support. It can be embedded into an application for simpler packaging or deployed as a standalone cluster.'
          },
          {
            title: 'Low latency',
            image: `${baseUrl}img/undraw_finish_line.svg`,
            imageAlign: 'top',
            content: 'Jet uses a combination of a directed acyclic graph (DAG) computation model,' +
          ' in-memory processing, data locality, partition mapping affinity, SP/SC queues, and green threads to' +
          ' achieve high throughput with predictable latency.'
          },
          {
            title: 'Resilient and Elastic',
            image: `${baseUrl}img/undraw_working_out.svg`,
            imageAlign: 'top',
            content: 'With Hazelcast Jet, it\'s easy to build fault-tolerant and elastic data processing pipelines. ' +
          'Jet keeps processing data without loss even when a node fails, and you can add more nodes that immediately ' +
          'start sharing the computation load.'
          }
        ]}
      </Block>
    );

    const Showcase = () => {
      if ((siteConfig.users || []).length === 0) {
        return null;
      }

      const showcase = siteConfig.users
        .filter(user => user.pinned)
        .map(user => (
          <a href={user.infoLink} key={user.infoLink}>
            <img src={user.image} alt={user.caption} title={user.caption} />
          </a>
        ));

      const pageUrl = page => baseUrl + (language ? `${language}/` : '') + page;

      return (
        <div className="productShowcaseSection paddingBottom">
          <h2>Who is Using This?</h2>
          <p>This project is used by all these people</p>
          <div className="logos">{showcase}</div>
          <div className="more-users">
            <a className="button" href={pageUrl('users.html')}>
              More {siteConfig.title} Users
            </a>
          </div>
        </div>
      );
    };

    return (
      <div>
        <HomeSplash siteConfig={siteConfig} language={language} />
        <div className="mainContainer">
          <Features />
          <Showcase />
        </div>
      </div>
    );
  }
}

module.exports = Index;
