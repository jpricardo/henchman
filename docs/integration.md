# Henchman Client Integration Guide

## Rules

**Register first.** Call `Register` before any other RPC. Pass your service's identifier as `instance_id` and configure your cache budget. All subsequent calls operate within that instance.

**Always set a 50ms deadline.** Henchman is intended for local network use. Every cache call must carry a deadline of 50ms. Calls that take longer than that indicate a problem with the cache server, not your data.

**Treat errors as cache misses.** The following conditions must never propagate as hard errors to your application — handle them as a cache miss and continue:
- `UNAVAILABLE` — server is down or not yet ready
- `DEADLINE_EXCEEDED` — call took longer than 50ms
- Connection refused — server is unreachable

Cache misses are not failures. Your application must function correctly without the cache.

---

## Node.js

### Install

```bash
npm install @jpricardo/henchman @grpc/grpc-js @bufbuild/protobuf
```

### Usage

```typescript
import * as grpc from "@grpc/grpc-js";
import { CacheClient } from "@jpricardo/henchman";

const client = new CacheClient(
  "localhost:50051",
  grpc.credentials.createInsecure()
);

const DEADLINE_MS = 50;

function deadline() {
  return { deadline: Date.now() + DEADLINE_MS };
}

function isCacheMiss(err: grpc.ServiceError): boolean {
  return (
    err.code === grpc.status.UNAVAILABLE ||
    err.code === grpc.status.DEADLINE_EXCEEDED
  );
}

// 1. Register
client.register(
  {
    instanceId: "my-service",
    config: {
      maxBytes: 64 * 1024 * 1024,
      maxKeys: 10000,
      defaultTtlMs: 60000,
      evictionPolicy: "lru",
      sweepIntervalMs: 5000,
    },
  },
  deadline(),
  (err) => {
    if (err && !isCacheMiss(err)) throw err;
  }
);

// 2. Set
client.set(
  { key: "hello", value: Buffer.from("world"), ttlMs: 0, invalidateAfterRead: false },
  deadline(),
  (err) => {
    if (err && !isCacheMiss(err)) throw err;
  }
);

// 3. Get
client.get({ key: "hello" }, deadline(), (err, res) => {
  if (err) {
    if (!isCacheMiss(err)) throw err;
    return; // treat as miss
  }
  if (res.found) {
    console.log(Buffer.from(res.value).toString());
  }
});
```

---

## C#

### Install

Add a project reference to `gen/csharp/Henchman.Grpc.csproj`, or pack and reference the NuGet artifact.

### Usage

```csharp
using Grpc.Net.Client;
using Henchman.V1;

var channel = GrpcChannel.ForAddress("http://localhost:50051");
var client = new Cache.CacheClient(channel);

var deadline = TimeSpan.FromMilliseconds(50);

bool IsCacheMiss(Grpc.Core.RpcException ex) =>
    ex.StatusCode is Grpc.Core.StatusCode.Unavailable
                  or Grpc.Core.StatusCode.DeadlineExceeded;

// 1. Register
try
{
    await client.RegisterAsync(new RegisterRequest
    {
        InstanceId = "my-service",
        Config = new InstanceConfig
        {
            MaxBytes = 64 * 1024 * 1024,
            MaxKeys = 10000,
            DefaultTtlMs = 60000,
            EvictionPolicy = "lru",
            SweepIntervalMs = 5000,
        }
    }, deadline: DateTime.UtcNow.Add(deadline));
}
catch (Grpc.Core.RpcException ex) when (IsCacheMiss(ex)) { }

// 2. Set
try
{
    await client.SetAsync(new SetRequest
    {
        Key = "hello",
        Value = Google.Protobuf.ByteString.CopyFromUtf8("world"),
    }, deadline: DateTime.UtcNow.Add(deadline));
}
catch (Grpc.Core.RpcException ex) when (IsCacheMiss(ex)) { }

// 3. Get
try
{
    var response = await client.GetAsync(
        new GetRequest { Key = "hello" },
        deadline: DateTime.UtcNow.Add(deadline));

    if (response.Found)
        Console.WriteLine(response.Value.ToStringUtf8());
}
catch (Grpc.Core.RpcException ex) when (IsCacheMiss(ex)) { } // treat as miss
```

---

## Java

### Install

Add `gen/java` as a subproject in `settings.gradle`, then depend on it:

```gradle
dependencies {
    implementation project(':henchman-grpc')
}
```

### Usage

```java
import henchman.v1.CacheGrpc;
import henchman.v1.CacheOuterClass.*;
import io.grpc.*;
import java.util.concurrent.TimeUnit;

ManagedChannel channel = ManagedChannelBuilder
    .forAddress("localhost", 50051)
    .usePlaintext()
    .build();

CacheGrpc.CacheBlockingStub stub = CacheGrpc.newBlockingStub(channel);

boolean isCacheMiss(StatusRuntimeException e) {
    return e.getStatus().getCode() == Status.Code.UNAVAILABLE
        || e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED;
}

// 1. Register
try {
    stub.withDeadlineAfter(50, TimeUnit.MILLISECONDS).register(
        RegisterRequest.newBuilder()
            .setInstanceId("my-service")
            .setConfig(InstanceConfig.newBuilder()
                .setMaxBytes(64 * 1024 * 1024)
                .setMaxKeys(10000)
                .setDefaultTtlMs(60000)
                .setEvictionPolicy("lru")
                .setSweepIntervalMs(5000)
                .build())
            .build());
} catch (StatusRuntimeException e) {
    if (!isCacheMiss(e)) throw e;
}

// 2. Set
try {
    stub.withDeadlineAfter(50, TimeUnit.MILLISECONDS).set(
        SetRequest.newBuilder()
            .setKey("hello")
            .setValue(com.google.protobuf.ByteString.copyFromUtf8("world"))
            .build());
} catch (StatusRuntimeException e) {
    if (!isCacheMiss(e)) throw e;
}

// 3. Get
try {
    GetResponse res = stub.withDeadlineAfter(50, TimeUnit.MILLISECONDS)
        .get(GetRequest.newBuilder().setKey("hello").build());

    if (res.getFound())
        System.out.println(res.getValue().toStringUtf8());
} catch (StatusRuntimeException e) {
    if (!isCacheMiss(e)) throw e; // treat as miss
}
```
