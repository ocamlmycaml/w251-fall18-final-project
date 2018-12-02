## Architecture:

![Architecture of the data pipeline](architecture.png "Architecture of the data pipeline")

### Tasks:
1. preprocess data using spark (note: if county data is not available, it has 0 pollutants) - **david, aditya**
2. load elasticsearch with pre-processed data - **praba**
3. convert address to FIPS code (just the county part) at query time - **anthony**
