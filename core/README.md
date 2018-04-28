# Auction Server Core

`auccore` is the main package to serve a auction server.

## Requirement

* Create `log` directory to save log file

## Example

```go
package main

import (
	"time"
	"github.com/zerozh/aucser/core"
)

func main() {
    conf := auccore.Config{
        StartTime:    time.Now(),
        HalfTime:     time.Now().Add(time.Second * 1800),
        EndTime:      time.Now().Add(time.Second * 3600),
        Capacity:     10000,
        WarningPrice: 863,
    }

    // Instancing a Exchange Server and Serve()
    exchange := auccore.NewExchange(conf)
    exchange.Serve()
    
    // Receive bids
    bid := &auccore.Bid{
        Client: 80001234,
        Price:  863,
    }
    exchange.Bid(bid)

    // Shutdown() After EndTime
    exchange.Shutdown()
}
```
