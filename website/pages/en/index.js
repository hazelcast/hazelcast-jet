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
            <Button href={docUrl('getting-started.html')}>Getting Started</Button>
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
            content: 'To achieve high throughputs with consistently low latency,' +
                ' Hazelcast Jet uses a combination of distributed Directed Acyclic Graphs (DAG)' +
                ' computation model, in-memory, data locality, partition mapping affinity, SP/SC Queues and Green Threads.\n' +
                '\n',
            title: 'Engineered for Performance',
          },
          {
            content: 'The performance of Hazelcast Jet Core can be boosted by using ' +
            'embedded Hazelcast In-Memory Data Grid (IMDG). Hazelcast IMDG provides elastic ' +
            'in-memory storage and is a great tool for publishing the results of the computation,' +
            ' or as a cache, for data sets to be used during the computation. Very low end-to-end ' +
            'latencies at extreme scale can be achieved this way.\n',
            title: 'Low Latency End-to-End',
          },
          {
            content: 'Hazelcast Jet is built on a low latency streaming core.' +
            ' Rather than accumulating the records into micro-batches and then processing,' +
            ' Hazelcast Jet processes the incoming records as soon as possible to accelerate performance.\n',
            title: 'Streaming Core',
          },
          {
            content: 'Hazelcast Jet allows you to classify records in a data stream based on' +
            ' the time stamp embedded in each record — the event time. Event time processing is a' +
            ' natural requirement as users are mostly interested in handling the data based on the' +
            ' time that the event originated (the event time). Event time processing is a first-class citizen in Jet.',
            title: 'Event Time Processing',
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
