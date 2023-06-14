---
title: Jet engine lives on in Hazelcast 5.x
description: Hazelcast Jet as standalone product reached end-of-life on April 21, 2023.
author: František Hartman
authorURL: https://www.linkedin.com/in/frantisek-hartman/
authorImageURL: https://i.stack.imgur.com/3X7wE.png 
---

At Hazelcast we have a policy of supporting a minor version for two years
since the initial release. For Hazelcast Jet 4.5 this period expired at the
end of April, marking an end of Hazelcast Jet as a standalone product.

Since 2021 our Jet engine, a stream processing system that can do 
stateful computations over massive amounts of data with consistent 
low latency, has been part of Hazelcast Platform 5.x.

We encourage every Hazelcast Jet user still using any of the Jet 4.x 
versions to upgrade to Hazelcast 5.x. There are some minor 
incompatibilities in how a Hazelcast instance is started, but e.g. the 
Pipeline API is fully backward compatible. See the 
[upgrade documentation](https://docs.hazelcast.com/hazelcast/latest/migrate/upgrading-from-jet) 
for details.   

Furthermore, here are a few more useful links:

[Hazelcast Documentation](https://docs.hazelcast.com/hazelcast/latest/index.html) - documentation for latest Hazelcast.

[Hazelcast GitHub](https://github.com/hazelcast/hazelcast/) - GitHub repository where you can report issues or contribute.

[Hazelcast Community Slack](slack.hazelcast.com) - the right place to interact with other members of Hazelcast community and Hazelcast developers.  

[StackOverflow](https://stackoverflow.com/questions/tagged/hazelcast) - if you have a question regarding use of Hazelcast or the Jet engine, this is the place to ask. 

[Hazelcast Google Group](https://groups.google.com/g/hazelcast) - another place where you can ask questions.

[Hazelcast website](https://hazelcast.com/) - our website with information about our products and services.
