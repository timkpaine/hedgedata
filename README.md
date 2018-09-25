# hedgeme_cache
Data caching

## DB
Caching uses MAN AHL's Arctic Library on Mongo to cache IEX data 

## Optimized data fetching
Can process 20 req/second (IEX limit) using ThreadPool

## Automatic data cleaning
Library can detect missing/erroneous data and refetch/fill holes


### Data Attribution
If you redistribute our API data:

- Cite IEX using the following text and link: “Data provided for free by [IEX](https://iextrading.com/developer).”
- Provide a link to https://iextrading.com/api-exhibit-a in your terms of service.
- Additionally, if you display our TOPS price data, cite “IEX Real-Time Price” near the price.

