# PFD_Demo
Patterns (or regex-like rules) are widely used to discover meta-knowledge in a given domain, e.g. a Year column should 
contain only four digits, and thus a value like "1980-" would be erroneous. In addition, data dependencies across columns, 
e.g. Postal Code  uniquely determines City is an important type of integrity constraints (ICs), which have 
been extensively studied. A promising, but not explored, 
direction is to leverage patterns to model the dependencies  
(or meta-knowledge)  between partial values  across columns. 
For instance, in an employee ID "F-9-107", "F" determines 
the finance department.

We propose a novel class of ICs, called pattern functional 
dependencies (PFDs), to model fine-grained data dependencies
gleaned from partial attribute values. These dependencies 
cannot be modeled using traditional ICs, such as (conditional) 
functional dependencies, which work on entire attribute values. 
We also present a set of axioms for the inference of PFDs, 
similar to Armstrong's axioms for FDs, as well as the analysis 
for the consistency and implication of a set of PFDs.  
Moreover, we devise an effective algorithm to automatically 
discover PFDs even in the presence of dirty data.

## Docker Installation
You need to install [Docker](https://www.docker.com/community-edition)
first, then proceed to the following instructions.

Get the code

    # clone using https
    git clone https://github.com/daqcri/PFD_Demo.git
    
    cd PFD_Demo
    
Build and run:

    docker compose up
    Open a web browser and write http://localhost:8050 in the address bar.

Alternatively run from docker image:

    docker run -it -p 8050:8050 qahtanaa/pfd_discovery
## Required Parameters
    Min support (K): the minimum number of records in which a pattern should appear to consider as a candidate for a PFD (better to use K > 3)
    Allowed violations(𝜹): the max ratio of patterns that are different from the main pattern to report PFD (𝜹 = 1 is a good choice).
    Min Coverage (𝜸): 
        coverage of a PFD is the number of records that contain its patterns. 
        A dependency between A and B is reported only if the coverage of the PFDs accumulate a coverage that is larger than a given threshold
        (from our empirical studies, using 𝜸 > 10 reduces the chance to report meaningless PFDs).

