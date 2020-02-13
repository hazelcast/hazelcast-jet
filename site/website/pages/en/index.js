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
            <Button href={docUrl('get-started/intro')}>Get Started</Button>
            <Button href="https://github.com/hazelcast/hazelcast-jet">View on GitHub</Button>
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

    const Features = () => (
      <Block layout="fourColumn">
        {[
          {
            title: 'Simple',
            image: `${baseUrl}img/undraw_relaxation.svg`,
            imageAlign: 'top',
            content: 'Jet is simple to set up. The nodes automatically discover each other to form a cluster.' +
            ' You can run Jet on your laptop and if you start it twice, you have a cluster. This is great' +
            ' for quick testing and also simplifies deployment and maintenance.',
          },
          {
            title: 'Runs everywhere',
            image: `${baseUrl}img/undraw_superhero.svg`,
            imageAlign: 'top',
            content: 'Jet is a single JAR without dependencies. It’s lightweight enough to run on small devices,' +
            ' and it’s cloud-native with Docker images and Kubernetes support. You can embed it into your' +
            ' application as just another dependency or deploy it as a standalone cluster.'
          },
          {
            title: 'Low latency',
            image: `${baseUrl}img/undraw_finish_line.svg`,
            imageAlign: 'top',
            content: 'Jet uses the directed acyclic graph (DAG) computation model, in-memory processing, data locality,' +
            ' partition affinity, non-blocking SP/SC queues and green threads to achieve high throughput with' +
            ' predictable latency.'
          },
          {
            title: 'Resilient and Elastic',
            image: `${baseUrl}img/undraw_working_out.svg`,
            imageAlign: 'top',
            content: 'With Jet it\'s easy to build fault-tolerant and elastic data processing pipelines.' +
          ' Jet keeps processing data without loss even when a node fails, and as soon as you add another node,' +
          ' it starts sharing the computation load.'
          }
        ]}
      </Block>
    );

    return (
      <div>
        <HomeSplash siteConfig={siteConfig} language={language} />
        <div className="mainContainer">
          <Features />
        </div>
      </div>
    );
  }
}

module.exports = Index;
