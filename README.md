# Java Full Stack Interview Prep

A structured reference guide for full-stack Java interviews covering core Java, Spring Boot, and Apache Kafka — with definitions, internals, trade-offs, code examples, and system-design patterns.

---

## Table of Contents

1. [How to Use This Guide](#how-to-use-this-guide)
2. [Java](#java)
   - [OOP & Language Fundamentals](#oop--language-fundamentals)
   - [Collections Framework](#collections-framework)
   - [Exception Handling](#exception-handling)
   - [Java 8+ Features](#java-8-features)
   - [Concurrency & Multithreading](#concurrency--multithreading)
   - [JVM, Memory & Garbage Collection](#jvm-memory--garbage-collection)
   - [Design Principles & Patterns](#design-principles--patterns)
3. [Spring Boot](#spring-boot)
   - [Core Concepts & IoC/DI](#core-concepts--iocdi)
   - [Bean Lifecycle & Scopes](#bean-lifecycle--scopes)
   - [Auto-Configuration](#auto-configuration)
   - [Web Layer & REST APIs](#web-layer--rest-apis)
   - [Data Access & JPA](#data-access--jpa)
   - [Transactions](#transactions)
   - [Spring Security](#spring-security)
   - [Testing](#testing)
   - [Production & Operations](#production--operations)
   - [Microservices & Spring Cloud](#microservices--spring-cloud)
4. [Apache Kafka](#apache-kafka)
   - [Fundamentals & Architecture](#fundamentals--architecture)
   - [Topics, Partitions & Offsets](#topics-partitions--offsets)
   - [Producers](#producers)
   - [Consumers & Consumer Groups](#consumers--consumer-groups)
   - [Delivery Semantics](#delivery-semantics)
   - [Replication & Durability](#replication--durability)
   - [Retention & Compaction](#retention--compaction)
   - [Kafka vs Alternatives](#kafka-vs-alternatives)
   - [Spring Kafka Integration](#spring-kafka-integration)
   - [System Design Patterns](#system-design-patterns)
5. [Cross-Topic Integration](#cross-topic-integration)
6. [Mock Interview Cheat Sheet](#mock-interview-cheat-sheet)
7. [3-Day Study Plan](#3-day-study-plan)
8. [Further Reading](#further-reading)

---

## How to Use This Guide

| Technique | How |
|-----------|-----|
| **30-second answer** | State definition → one trade-off → one real example |
| **Deep dive** | Expand with internals when interviewer says "go deeper" |
| **System design** | Always mention: ordering, idempotency, failure handling, observability |
| **Stories** | Prepare 2 narratives: one debugging incident, one event-driven design |

**Answer framework:** *What it is → Why it exists → When to use / not use → Pitfalls*

---

# Java

---

## OOP & Language Fundamentals

### Q1. What are the four pillars of OOP?

| Pillar | Definition | Example |
|--------|------------|---------|
| **Encapsulation** | Hide internal state; expose controlled access via methods | Private fields + getters/setters |
| **Inheritance** | IS-A relationship; reuse and extend behavior | `class Dog extends Animal` |
| **Polymorphism** | Same interface, different implementations | `Animal a = new Dog(); a.speak()` |
| **Abstraction** | Hide complexity; expose essential behavior | `interface PaymentGateway { void pay(); }` |

**Interview tip:** Mention **composition over inheritance** — favor `has-a` (wrapping) over deep inheritance hierarchies to avoid fragile base class problems.

---

### Q2. `abstract class` vs `interface`?

| Aspect | Abstract Class | Interface |
|--------|----------------|-----------|
| Fields | Can have instance fields | Only `public static final` constants (pre-Java 9) |
| Methods | Abstract + concrete | Abstract (implicit) + default/static (Java 8+) |
| Inheritance | Single inheritance | Multiple interfaces |
| Constructor | Yes | No |
| Use when | Shared state + partial implementation | Define a capability/contract |

```java
// Interface — capability
public interface Notifiable {
    void send(String message);
    default void log(String msg) { System.out.println(msg); }
}

// Abstract class — shared template
public abstract class BaseEntity {
    protected Long id;
    public abstract void validate();
}
```

**Java 8+:** Interfaces can have `default` and `static` methods. **Java 9+:** private methods in interfaces.

**Modern guidance:** Prefer interfaces for behavior contracts; use abstract classes sparingly when subclasses share substantial code and state.

---

### Q3. `==` vs `.equals()` vs `hashCode()`?

| Operator/Method | Compares | Use |
|-----------------|----------|-----|
| `==` | Reference identity (objects) or value (primitives) | Primitive comparison |
| `.equals()` | Logical equality (when overridden) | Object value comparison |
| `hashCode()` | Hash bucket for collections | Must be consistent with `equals()` |

**Contract (must override together):**
1. If `a.equals(b)` → `a.hashCode() == b.hashCode()`
2. Reflexive, symmetric, transitive, consistent
3. `equals(null)` → false

```java
@Override
public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Person p)) return false;
    return Objects.equals(id, p.id);
}

@Override
public int hashCode() {
    return Objects.hash(id);
}
```

**Pitfall:** Using mutable objects as `HashMap` keys — if key fields change after insertion, bucket lookup breaks.

---

### Q4. Why is `String` immutable?

1. **String pool** — literals can be interned and reused safely
2. **Thread safety** — no synchronization needed
3. **Security** — credentials/URLs can't be modified after passing
4. **Hash stability** — `hashCode()` cached; safe as `HashMap` key

```java
// BAD — creates N objects in loop
String s = "";
for (int i = 0; i < 1000; i++) s += i;

// GOOD
StringBuilder sb = new StringBuilder();
for (int i = 0; i < 1000; i++) sb.append(i);
```

**`String` vs `StringBuilder` vs `StringBuffer`:**
- `String` — immutable
- `StringBuilder` — mutable, not thread-safe (preferred)
- `StringBuffer` — mutable, synchronized (legacy)

---

### Q5. `final`, `finally`, `finalize`?

| Keyword | Purpose |
|---------|---------|
| `final` variable | Cannot reassign (reference fixed; object mutable) |
| `final` method | Cannot override |
| `final` class | Cannot extend (`String`, `Integer`) |
| `finally` | Block always executed (unless `System.exit()`) |
| `finalize()` | **Deprecated** — GC callback; unpredictable, don't use |

**Prefer try-with-resources** over `finally` for closing resources:

```java
try (InputStream in = Files.newInputStream(path)) {
    // use stream
} // auto-closed
```

---

### Q6. `static` keyword — what does it mean?

- Belongs to the **class**, not instance
- Loaded when class is loaded (method area / metaspace)
- Cannot access non-static members directly from static context
- Static blocks run once at class initialization

**Common uses:** utility methods, constants, factory methods, singleton (holder idiom).

---

### Q7. Pass-by-value vs pass-by-reference?

Java is **always pass-by-value**:
- Primitives: value copied
- Objects: **reference value** copied (both references point to same object)

```java
void modify(List<String> list) {
    list.add("x");     // visible to caller
    list = new ArrayList<>(); // caller's reference unchanged
}
```

---

### Q8. `Comparable` vs `Comparator`?

| | Comparable | Comparator |
|---|------------|------------|
| Package | `java.lang` | `java.util` |
| Method | `compareTo(T o)` | `compare(T a, T b)` |
| Implementation | Inside class | External/separate |
| Sorting | Natural order | Custom/multiple orders |

```java
// Comparable
public class Employee implements Comparable<Employee> {
    public int compareTo(Employee other) {
        return this.salary.compareTo(other.salary);
    }
}

// Comparator
Comparator<Employee> byName = Comparator.comparing(Employee::getName);
```

---

## Collections Framework

### Q9. Collections hierarchy overview

```
Iterable
  └── Collection
        ├── List (ordered, duplicates)
        │     ├── ArrayList
        │     ├── LinkedList
        │     └── Vector (legacy, synchronized)
        ├── Set (no duplicates)
        │     ├── HashSet
        │     ├── LinkedHashSet
        │     └── TreeSet (sorted)
        └── Queue
              ├── PriorityQueue
              └── Deque (ArrayDeque)

Map (separate hierarchy)
  ├── HashMap
  ├── LinkedHashMap
  ├── TreeMap
  └── ConcurrentHashMap
```

---

### Q10. `ArrayList` vs `LinkedList`?

| Operation | ArrayList | LinkedList |
|-----------|-----------|------------|
| Random access `get(i)` | O(1) | O(n) |
| Insert at end | O(1) amortized | O(1) |
| Insert/delete middle | O(n) | O(1) if you have node |
| Memory | Less overhead | More (node pointers) |

**Default choice: `ArrayList`** unless you have proven need for frequent head/tail mutations.

---

### Q11. `HashMap` internals (deep dive)

**Structure (Java 8+):**
1. Array of buckets (capacity, power of 2)
2. `hash(key)` → spread bits → `index = (n-1) & hash`
3. Collision: linked list in bucket
4. If bucket size > 8 AND table size ≥ 64 → treeify to Red-Black Tree
5. If tree size < 6 → untreeify back to list

**Key parameters:**
| Parameter | Default | Meaning |
|-----------|---------|---------|
| Initial capacity | 16 | Bucket array size |
| Load factor | 0.75 | Resize when 75% full |
| Resize | 2× capacity | Rehash all entries |

**Put flow:**
```
hash(key) → index → if null, insert
                 → if same key, replace value
                 → else chain/tree → resize if needed
```

**Thread safety:** Not thread-safe. Concurrent modification during iteration → `ConcurrentModificationException` (fail-fast iterator).

**Null:** One null key allowed; multiple null values allowed.

---

### Q12. `HashMap` vs `Hashtable` vs `ConcurrentHashMap`?

| | HashMap | Hashtable | ConcurrentHashMap |
|---|---------|-----------|-------------------|
| Thread-safe | No | Yes (synchronized methods) | Yes (fine-grained) |
| Null key/value | Yes | No | No |
| Performance | Fast single-thread | Poor (global lock) | High concurrency |
| Iterator | Fail-fast | Fail-fast | Weakly consistent |

**ConcurrentHashMap (Java 8+):**
- CAS for uncontended buckets
- `synchronized` on bucket head for updates
- No segment locks (pre-Java 8 had segments)
- `compute`, `merge` atomic operations

---

### Q13. `HashSet` internals?

`HashSet` is backed by `HashMap` — elements are keys; dummy `PRESENT` object as value.

**Ordering:**
- `HashSet` — no order
- `LinkedHashSet` — insertion order (linked list through entries)
- `TreeSet` — sorted (`TreeMap` backing), O(log n)

---

### Q14. When to use which collection?

| Need | Choice |
|------|--------|
| General list | `ArrayList` |
| Frequent insert/delete at ends | `ArrayDeque` |
| Unique elements, fast lookup | `HashSet` |
| Sorted unique | `TreeSet` |
| Key-value, single-threaded | `HashMap` |
| Key-value, concurrent | `ConcurrentHashMap` |
| Sorted map | `TreeMap` |
| Insertion-order iteration | `LinkedHashMap` / `LinkedHashSet` |

---

## Exception Handling

### Q15. Checked vs unchecked exceptions?

| Type | Extends | Must declare/handle? | Examples |
|------|---------|---------------------|----------|
| Checked | `Exception` (not RuntimeException) | Yes | `IOException`, `SQLException` |
| Unchecked | `RuntimeException` | No | `NullPointerException`, `IllegalArgumentException` |

**Modern practice:**
- Use unchecked for programming errors and business rule violations
- Use checked at system boundaries (I/O, JDBC) or wrap in unchecked domain exceptions
- Spring `@Transactional` rolls back on unchecked by default, not checked

```java
// Custom business exception (unchecked)
public class OrderNotFoundException extends RuntimeException {
    public OrderNotFoundException(Long id) {
        super("Order not found: " + id);
    }
}
```

---

### Q16. `try-catch-finally` execution rules?

1. `finally` runs even if exception thrown (unless `System.exit()`)
2. If `try` has `return`, `finally` still runs before return
3. If `finally` also has `return`, it suppresses `try` return (avoid this)

**try-with-resources:** Any resource implementing `AutoCloseable` closed in reverse order.

---

## Java 8+ Features

### Q17. Functional interfaces and lambda expressions

**Functional interface:** exactly one abstract method (SAM).

| Interface | Method | Use |
|-----------|--------|-----|
| `Predicate<T>` | `boolean test(T t)` | Filter |
| `Function<T,R>` | `R apply(T t)` | Transform |
| `Consumer<T>` | `void accept(T t)` | Side effect |
| `Supplier<T>` | `T get()` | Factory/lazy |
| `BiFunction<T,U,R>` | `R apply(T t, U u)` | Two-arg transform |

```java
List<String> names = people.stream()
    .filter(p -> p.getAge() > 18)
    .map(Person::getName)
    .sorted()
    .toList();
```

**Method references:** `Person::getName` ≡ `p -> p.getName()`

---

### Q18. Stream API (deep dive)

**Characteristics:**
- **Declarative** pipeline
- **Lazy** — intermediate ops don't run until terminal op
- **Not reusable** — one-shot
- **No modification** of source during stream

**Operation types:**
```
Source → intermediate* → terminal
         (lazy)         (eager, triggers pipeline)
```

| Intermediate | Terminal |
|--------------|----------|
| `filter`, `map`, `flatMap` | `collect`, `forEach`, `reduce` |
| `sorted`, `distinct`, `peek` | `count`, `min`, `max` |
| `limit`, `skip` | `findFirst`, `anyMatch` |

```java
Map<String, Long> countByDept = employees.stream()
    .collect(Collectors.groupingBy(
        Employee::getDepartment,
        Collectors.counting()
    ));
```

**Parallel streams:**
- Use only for large, CPU-bound, associative operations
- ForkJoinPool.commonPool()
- Pitfalls: shared mutable state, wrong spliterator, ordering overhead

---

### Q19. `Optional` — best practices

**Good:**
```java
public Optional<User> findById(Long id) { ... }

return findById(id)
    .map(User::getEmail)
    .orElse("unknown@example.com");
```

**Bad:**
```java
Optional.ofNullable(getUser()); // as field
if (optional.isPresent()) { ... } // prefer map/orElse
optional.get(); // without check — NPE risk
```

---

### Q20. `Record` classes (Java 16+)

Immutable data carriers with auto-generated constructor, equals, hashCode, toString.

```java
public record OrderEvent(Long orderId, String status, Instant timestamp) {}
```

Use for DTOs, events, value objects. Not a replacement for entities with JPA (mutable/lazy loading needs).

---

### Q21. `sealed` classes (Java 17+)

Restrict which classes can extend/implement:

```java
public sealed interface Payment permits CreditCard, PayPal, BankTransfer {}
```

Enables exhaustive `switch` pattern matching.

---

## Concurrency & Multithreading

### Q22. Thread lifecycle

```
NEW → RUNNABLE ⇄ BLOCKED/WAITING/TIMED_WAITING → TERMINATED
```

| State | Cause |
|-------|-------|
| BLOCKED | Waiting for monitor lock |
| WAITING | `wait()`, `join()`, `park()` |
| TIMED_WAITING | `sleep()`, `wait(timeout)` |

---

### Q23. Creating threads — options

| Approach | Recommendation |
|----------|----------------|
| Extend `Thread` | Avoid |
| Implement `Runnable` | OK for simple cases |
| `ExecutorService` | **Production standard** |
| `Callable` + `Future` | When you need return value |
| `CompletableFuture` | Async composition (Java 8+) |

```java
ExecutorService pool = Executors.newFixedThreadPool(10);
Future<String> future = pool.submit(() -> fetchData());
String result = future.get(5, TimeUnit.SECONDS);
pool.shutdown();
```

**Better:** configure `ThreadPoolExecutor` explicitly:

```java
ThreadPoolExecutor executor = new ThreadPoolExecutor(
    4,                      // core
    8,                      // max
    60L, TimeUnit.SECONDS,
    new LinkedBlockingQueue<>(100),
    new ThreadPoolExecutor.CallerRunsPolicy()
);
```

---

### Q24. `synchronized` vs `Lock` vs `volatile`?

| Mechanism | Guarantees | Use |
|-----------|------------|-----|
| `synchronized` | Mutual exclusion + visibility | Simple critical sections |
| `ReentrantLock` | Same + tryLock, fairness, conditions | Advanced locking |
| `volatile` | Visibility only (happens-before) | Single flag/status field |
| `Atomic*` classes | CAS, lock-free | Counters, references |

**`volatile` does NOT make `i++` atomic** — use `AtomicInteger`.

---

### Q25. Deadlock — causes and prevention

**Four Coffman conditions (all required for deadlock):**
1. Mutual exclusion
2. Hold and wait
3. No preemption
4. Circular wait

**Prevention:**
- Lock ordering (always acquire A then B)
- `tryLock` with timeout
- Smaller critical sections
- Avoid nested locks

**Detection:** thread dumps (`jstack`), VisualVM, actuator `/threaddump`.

---

### Q26. `ConcurrentHashMap` advanced operations

```java
map.computeIfAbsent(key, k -> expensiveLoad(k));
map.merge(key, 1, Integer::sum);
map.putIfAbsent(key, value);
```

Prefer these over `get` + `put` for atomicity.

---

### Q27. `CompletableFuture` patterns

```java
CompletableFuture<String> future = CompletableFuture
    .supplyAsync(() -> fetchUser())
    .thenApply(user -> enrich(user))
    .thenCompose(user -> fetchOrdersAsync(user))
    .exceptionally(ex -> handleError(ex));

// Combine
CompletableFuture.allOf(f1, f2, f3).join();
```

---

### Q28. ThreadLocal

Per-thread variable copy. Use cases: request context, `SimpleDateFormat` (legacy), tracing IDs.

**Memory leak risk:** In thread pools, threads are reused — always `remove()` in `finally` when done.

```java
private static final ThreadLocal<String> ctx = new ThreadLocal<>();

try {
    ctx.set(correlationId);
    process();
} finally {
    ctx.remove();
}
```

---

## JVM, Memory & Garbage Collection

### Q29. JVM memory areas

```
┌─────────────────────────────────────┐
│            JVM Memory               │
├──────────────┬──────────────────────┤
│ Thread Stacks│ Heap                │
│ (per thread) │ ├── Young Gen       │
│              │ │   Eden + Survivor │
│              │ └── Old Gen         │
├──────────────┴──────────────────────┤
│ Metaspace (class metadata, Java 8+) │
│ (was PermGen in Java 7)             │
├─────────────────────────────────────┤
│ Code Cache, Direct Memory (off-heap)│
└─────────────────────────────────────┘
```

| Area | Stores |
|------|--------|
| Stack | Frames, local vars, operand stack |
| Heap | Objects, arrays |
| Metaspace | Class definitions, static metadata |

---

### Q30. Garbage collection basics

**Minor GC (Young gen):**
- Eden fills → live objects copied to Survivor
- After several cycles → promoted to Old gen

**Major/Full GC:**
- Old gen cleanup; STW (stop-the-world) pauses

**Common collectors:**

| Collector | Profile |
|-----------|---------|
| G1 (default Java 9+) | Balanced, region-based |
| ZGC | Ultra-low latency (Java 15+) |
| Shenandoah | Low pause concurrent |
| Parallel GC | Throughput-focused batch |

**Tuning flags:**
```
-Xms512m -Xmx2g
-XX:+UseG1GC
-XX:+HeapDumpOnOutOfMemoryError
-Xlog:gc*
```

---

### Q31. Memory leaks in Java

Java doesn't leak memory like C — but **unreachable objects retained by references**:

| Cause | Example |
|-------|---------|
| Static collections | Cache never cleared |
| Listeners not removed | Event bus subscribers |
| ThreadLocal not cleared | Pooled threads |
| Unclosed resources | DB connections, streams |
| Custom class loaders | hot deploy in app servers |

**Tools:** heap dump (`.hprof`), Eclipse MAT, VisualVM, async-profiler.

---

### Q32. Strong, soft, weak, phantom references

| Type | GC behavior |
|------|-------------|
| Strong | Never collected while reachable |
| Soft | Collected when memory pressure (caches) |
| Weak | Collected at next GC (`WeakHashMap`) |
| Phantom | After finalization; used for cleanup tracking |

---

## Design Principles & Patterns

### Q33. SOLID principles

| Principle | Meaning | Violation symptom |
|-----------|---------|-------------------|
| **S** — Single Responsibility | One reason to change | God classes |
| **O** — Open/Closed | Open for extension, closed for modification | Modifying core for every feature |
| **L** — Liskov Substitution | Subtypes replaceable for base | Broken overrides |
| **I** — Interface Segregation | Small, focused interfaces | Fat interfaces |
| **D** — Dependency Inversion | Depend on abstractions | `new ConcreteService()` everywhere |

---

### Q34. Common patterns (interview favorites)

| Pattern | Purpose | Spring example |
|---------|---------|----------------|
| Singleton | One instance | Spring beans (default) |
| Factory | Object creation | `BeanFactory` |
| Strategy | Interchangeable algorithms | `PaymentStrategy` |
| Observer | Event notification | `ApplicationEventPublisher` |
| Proxy | Intercept calls | Spring AOP, `@Transactional` |
| Template Method | Algorithm skeleton | `JdbcTemplate`, `RestTemplate` |
| Builder | Complex object construction | Lombok `@Builder` |
| Decorator | Add behavior dynamically | `HttpServletRequestWrapper` |

---

### Q35. Singleton — thread-safe implementations

```java
// 1. Enum (Joshua Bloch — best)
public enum Database {
    INSTANCE;
    public Connection getConnection() { ... }
}

// 2. Holder idiom (lazy, thread-safe)
public class Singleton {
    private Singleton() {}
    private static class Holder {
        static final Singleton INSTANCE = new Singleton();
    }
    public static Singleton getInstance() { return Holder.INSTANCE; }
}

// 3. Double-checked locking
private static volatile Singleton instance;
public static Singleton getInstance() {
    if (instance == null) {
        synchronized (Singleton.class) {
            if (instance == null) instance = new Singleton();
        }
    }
    return instance;
}
```

---

# Spring Boot

---

## Core Concepts & IoC/DI

### Q1. What is Spring? What is Spring Boot?

**Spring Framework:**
- Inversion of Control (IoC) container
- Dependency Injection (DI)
- Aspect-Oriented Programming (AOP)
- Modular ecosystem (MVC, Data, Security, etc.)

**Spring Boot:**
- Opinionated auto-configuration on top of Spring
- Embedded servers (Tomcat, Jetty, Undertow)
- Starter dependencies (curated BOM)
- Production-ready features (Actuator, metrics)
- Minimal XML; convention over configuration

```
@SpringBootApplication
  = @Configuration
  + @EnableAutoConfiguration
  + @ComponentScan
```

---

### Q2. Inversion of Control vs Dependency Injection?

| Concept | Definition |
|---------|------------|
| **IoC** | Framework controls object creation and lifecycle (Hollywood Principle: "don't call us, we'll call you") |
| **DI** | Implementation of IoC — dependencies injected rather than constructed internally |

**Without DI:**
```java
public class OrderService {
    private PaymentClient client = new StripeClient(); // tight coupling
}
```

**With DI:**
```java
@Service
public class OrderService {
    private final PaymentClient client;
    public OrderService(PaymentClient client) { // constructor injection
        this.client = client;
    }
}
```

---

### Q3. Types of dependency injection?

| Type | Annotation | Recommendation |
|------|------------|----------------|
| Constructor | implicit / `@Autowired` on constructor | **Preferred** — immutable, testable |
| Setter | `@Autowired` on setter | Optional dependencies |
| Field | `@Autowired` on field | Avoid — hard to test, hides dependencies |

```java
@Service
public class UserService {
    private final UserRepository repo;
    private EmailService email; // optional

    public UserService(UserRepository repo) {
        this.repo = repo;
    }

    @Autowired(required = false)
    public void setEmailService(EmailService email) {
        this.email = email;
    }
}
```

---

### Q4. `@Component` vs `@Service` vs `@Repository` vs `@Controller`?

All are **stereotypes** of `@Component` — functionally similar for component scanning.

| Annotation | Layer | Special behavior |
|------------|-------|------------------|
| `@Component` | Generic | None |
| `@Service` | Business logic | Semantic only |
| `@Repository` | Data access | Exception translation (`DataAccessException`) |
| `@Controller` | Web (MVC) | Returns view name |
| `@RestController` | REST API | `@Controller` + `@ResponseBody` |

---

### Q5. `@Autowired` vs `@Qualifier` vs `@Primary`?

When multiple beans implement the same type:

```java
@Primary
@Bean
public CacheManager redisCache() { ... }

@Bean
public CacheManager localCache() { ... }

// Injection site
@Autowired
@Qualifier("localCache")
private CacheManager cache;
```

Resolution order: `@Qualifier` > `@Primary` > bean name match > failure.

---

### Q6. `@Bean` vs `@Component`?

| | `@Component` | `@Bean` |
|---|--------------|---------|
| Where | On your class | In `@Configuration` class |
| Control | Spring creates via scanning | You control instantiation |
| Use | Your code | Third-party libraries, conditional setup |

```java
@Configuration
public class AppConfig {
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
```

---

## Bean Lifecycle & Scopes

### Q7. Bean lifecycle (full)

```
1. Instantiation (constructor)
2. Populate properties (DI)
3. BeanNameAware, BeanFactoryAware, ApplicationContextAware
4. BeanPostProcessor.postProcessBeforeInitialization()
5. @PostConstruct / InitializingBean.afterPropertiesSet()
6. Custom init-method
7. BeanPostProcessor.postProcessAfterInitialization()
8. Bean ready for use
--- shutdown ---
9. @PreDestroy / DisposableBean.destroy()
10. Custom destroy-method
```

```java
@Component
public class CacheWarmer {
    @PostConstruct
    public void warm() { /* load cache on startup */ }

    @PreDestroy
    public void flush() { /* cleanup */ }
}
```

---

### Q8. Bean scopes

| Scope | Description | Use case |
|-------|-------------|----------|
| `singleton` (default) | One per IoC container | Stateless services |
| `prototype` | New instance every time | Stateful, non-shared |
| `request` | One per HTTP request | Web apps |
| `session` | One per HTTP session | User-specific web state |
| `application` | One per ServletContext | Shared web app state |

**Note:** Injecting prototype into singleton gives one prototype instance unless you use `ObjectProvider<T>` or `@Scope(proxyMode = TARGET_CLASS)`.

---

## Auto-Configuration

### Q9. How does Spring Boot auto-configuration work?

1. `@EnableAutoConfiguration` imports `AutoConfigurationImportSelector`
2. Reads `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
3. Each auto-config class uses **conditional annotations**
4. Creates beans only when conditions match

**Key conditional annotations:**

| Annotation | Condition |
|------------|-----------|
| `@ConditionalOnClass` | Class on classpath |
| `@ConditionalOnMissingBean` | No user-defined bean of type |
| `@ConditionalOnProperty` | Property value matches |
| `@ConditionalOnWebApplication` | Web app context |
| `@ConditionalOnExpression` | SpEL expression |

**Debugging:** `--debug` or `logging.level.org.springframework.boot.autoconfigure=DEBUG` shows positive/negative matches report.

---

### Q10. `@Configuration` vs `@Component` for `@Bean` methods?

`@Configuration` classes are **CGLIB-proxied** — `@Bean` method inter-calls return the same singleton.

```java
@Configuration
public class Config {
    @Bean
    public ServiceA serviceA() { return new ServiceA(repo()); }

    @Bean
    public Repo repo() { return new Repo(); }

    @Bean
    public ServiceB serviceB() { return new ServiceB(repo()); }
    // repo() called from serviceA and serviceB → SAME bean instance
}
```

With `@Component`, inter-method calls are NOT proxied — multiple instances possible.

---

### Q11. Externalized configuration

**Priority (highest wins):**
1. Command line args
2. `SPRING_APPLICATION_JSON`
3. Java system properties
4. OS environment variables
5. `application-{profile}.properties/yml`
6. `application.properties/yml`

```yaml
# application.yml
spring:
  profiles:
    active: ${SPRING_PROFILES_ACTIVE:dev}

app:
  kafka:
    topic: orders
```

**`@Value` vs `@ConfigurationProperties`:**

```java
@ConfigurationProperties(prefix = "app.kafka")
@Validated
public record KafkaProps(
    @NotBlank String topic,
    @Min(1) int retries
) {}
```

Prefer `@ConfigurationProperties` for type-safe, validated, grouped config.

---

## Web Layer & REST APIs

### Q12. Request flow in Spring MVC

```
HTTP Request
  → Servlet Container (Tomcat)
  → Filter chain (Security, CORS, etc.)
  → DispatcherServlet
  → HandlerMapping → Controller method
  → HandlerAdapter invokes method
  → Return value → HttpMessageConverter (JSON)
  → HTTP Response
```

---

### Q13. Key REST annotations

```java
@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

  @GetMapping("/{id}")
  public ResponseEntity<OrderDto> get(@PathVariable Long id) { ... }

  @PostMapping
  public ResponseEntity<OrderDto> create(
      @Valid @RequestBody CreateOrderRequest req) { ... }

  @GetMapping
  public Page<OrderDto> list(
      @RequestParam(defaultValue = "0") int page,
      @RequestParam(defaultValue = "20") int size) { ... }
}
```

| Annotation | Purpose |
|------------|---------|
| `@PathVariable` | URI template variable |
| `@RequestParam` | Query parameter |
| `@RequestBody` | Deserialize JSON body |
| `@RequestHeader` | HTTP header |
| `@ResponseStatus` | Set HTTP status |
| `@Valid` | Trigger Bean Validation |

---

### Q14. Global exception handling

```java
@RestControllerAdvice
public class GlobalExceptionHandler {

  @ExceptionHandler(OrderNotFoundException.class)
  public ResponseEntity<ErrorResponse> handleNotFound(OrderNotFoundException ex) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND)
        .body(new ErrorResponse("ORDER_NOT_FOUND", ex.getMessage()));
  }

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex) {
    String msg = ex.getBindingResult().getFieldErrors().stream()
        .map(e -> e.getField() + ": " + e.getDefaultMessage())
        .collect(Collectors.joining(", "));
    return ResponseEntity.badRequest().body(new ErrorResponse("VALIDATION_ERROR", msg));
  }
}
```

---

### Q15. Filter vs Interceptor vs AOP?

| Layer | Runs at | Use |
|-------|---------|-----|
| **Filter** | Servlet container (before Spring) | Encoding, logging, security tokens |
| **HandlerInterceptor** | Spring MVC (pre/post/afterCompletion) | Auth checks, timing, MVC context |
| **AOP** | Method join points | Transactions, logging, metrics |

```
Filter → DispatcherServlet → Interceptor → Controller → Service
                                    ↑
                                   AOP wraps service/repository methods
```

---

## Data Access & JPA

### Q16. JPA vs Hibernate vs Spring Data JPA

| Layer | Role |
|-------|------|
| **JPA** | Java Persistence API — specification |
| **Hibernate** | JPA implementation (most common) |
| **Spring Data JPA** | Repository abstraction on top of JPA |

```java
public interface OrderRepository extends JpaRepository<Order, Long> {
    List<Order> findByCustomerId(Long customerId);

    @Query("SELECT o FROM Order o JOIN FETCH o.items WHERE o.id = :id")
    Optional<Order> findWithItems(@Param("id") Long id);
}
```

---

### Q17. Entity relationships

| Annotation | Ownership | Default fetch |
|------------|-----------|---------------|
| `@OneToMany` | Parent side | LAZY |
| `@ManyToOne` | Child side (FK) | EAGER |
| `@ManyToMany` | Join table | LAZY |
| `@OneToOne` | Either side | EAGER |

**Best practice:** Set `@ManyToOne(fetch = LAZY)` explicitly. Avoid bidirectional when unidirectional suffices.

---

### Q18. N+1 problem (deep dive)

**Problem:** 1 query for N parent rows + N queries for children.

```java
// Triggers N+1 if items are LAZY
List<Order> orders = orderRepository.findAll();
orders.forEach(o -> o.getItems().size());
```

**Solutions:**

| Approach | How |
|----------|-----|
| JOIN FETCH | `@Query("SELECT o FROM Order o JOIN FETCH o.items")` |
| `@EntityGraph` | `@EntityGraph(attributePaths = "items")` on query method |
| Batch fetching | `hibernate.default_batch_fetch_size=16` |
| DTO projection | Query only needed columns |

---

### Q19. `LazyInitializationException`

Occurs when lazy association accessed **outside** an active persistence context (session).

**Fixes:**
1. `@Transactional` on service method that accesses lazy data
2. JOIN FETCH in query
3. `OpenEntityManagerInViewFilter` (default in Spring Boot — masks problem in web layer; controversial for APIs)
4. DTO mapping inside transaction

---

### Q20. `EntityManager` flush and clear

| Operation | Effect |
|-----------|--------|
| `flush()` | Sync persistence context to DB (SQL executed) |
| `clear()` | Detach all managed entities |
| `persist()` | Make transient entity managed |
| `merge()` | Attach detached entity (copy) |
| `remove()` | Delete managed entity |

---

## Transactions

### Q21. `@Transactional` deep dive

Spring uses **AOP proxy** around `@Transactional` beans.

```java
@Service
public class OrderService {
    @Transactional
    public Order placeOrder(CreateOrderRequest req) {
        Order order = orderRepository.save(new Order(req));
        inventoryService.reserve(order); // participates in same tx
        return order;
    }

    @Transactional(readOnly = true)
    public Order getOrder(Long id) {
        return orderRepository.findById(id).orElseThrow();
    }
}
```

| Property | Default | Notes |
|----------|---------|-------|
| `propagation` | `REQUIRED` | Join existing or create new |
| `isolation` | DEFAULT (DB default) | Usually READ_COMMITTED |
| `rollbackFor` | RuntimeException + Error | NOT checked exceptions |
| `readOnly` | false | Optimization hint for queries |
| `timeout` | -1 | Seconds before rollback |

**Propagation types (know these):**

| Propagation | Behavior |
|-------------|----------|
| `REQUIRED` | Join current tx or create new |
| `REQUIRES_NEW` | Always new tx; suspends current |
| `NESTED` | Savepoint within current tx |
| `MANDATORY` | Must have existing tx; else exception |
| `NOT_SUPPORTED` | Run non-transactional; suspend current |
| `NEVER` | Must NOT have tx; else exception |
| `SUPPORTS` | Use tx if exists; else non-transactional |

---

### Q22. Self-invocation trap

`@Transactional` on method called **from same class** bypasses proxy:

```java
@Service
public class OrderService {
    public void process() {
        this.save(); // NO TRANSACTION — direct call, no proxy
    }

    @Transactional
    public void save() { ... }
}
```

**Fix:** Extract to another bean, or use `TransactionTemplate`, or self-inject.

---

### Q23. Transaction isolation levels

| Level | Dirty read | Non-repeatable read | Phantom read |
|-------|------------|---------------------|--------------|
| READ_UNCOMMITTED | Yes | Yes | Yes |
| READ_COMMITTED | No | Yes | Yes |
| REPEATABLE_READ | No | No | Yes |
| SERIALIZABLE | No | No | No |

---

## Spring Security

### Q24. Authentication vs Authorization

| | Authentication | Authorization |
|---|----------------|---------------|
| Question | Who are you? | What can you do? |
| Mechanism | Login, JWT, OAuth2 | Roles, permissions, `@PreAuthorize` |
| Spring | `AuthenticationManager` | `AccessDecisionManager` |

---

### Q25. Spring Security filter chain

```
Request
  → SecurityContextPersistenceFilter
  → UsernamePasswordAuthenticationFilter (form login)
  → BearerTokenAuthenticationFilter (JWT/OAuth2)
  → AuthorizationFilter
  → Controller
```

**JWT flow (stateless):**
1. Client sends `Authorization: Bearer <token>`
2. Filter validates signature, expiry, claims
3. Sets `SecurityContextHolder.getContext().setAuthentication(...)`
4. `@PreAuthorize("hasRole('ADMIN')")` evaluated

```java
@PreAuthorize("hasAuthority('ORDER_READ')")
@GetMapping("/{id}")
public OrderDto get(@PathVariable Long id) { ... }
```

---

### Q26. CSRF, CORS

| | CSRF | CORS |
|---|------|------|
| Threat | Cross-site request forgery | Cross-origin browser requests |
| REST API | Disable CSRF for stateless JWT APIs | Configure allowed origins |
| Config | `csrf.disable()` for APIs | `CorsConfigurationSource` bean |

---

## Testing

### Q27. Spring Boot testing slices

| Annotation | Loads | Use |
|------------|-------|-----|
| `@SpringBootTest` | Full context | Integration tests |
| `@WebMvcTest` | Web layer only | Controller unit tests |
| `@DataJpaTest` | JPA + in-memory DB | Repository tests |
| `@JsonTest` | JSON serialization | DTO mapping |
| `@MockBean` | Replace bean in context | Mock dependencies |

```java
@WebMvcTest(OrderController.class)
class OrderControllerTest {
    @Autowired MockMvc mvc;
    @MockBean OrderService orderService;

    @Test
    void getOrder() throws Exception {
        when(orderService.get(1L)).thenReturn(new OrderDto(1L, "PENDING"));
        mvc.perform(get("/api/v1/orders/1"))
           .andExpect(status().isOk())
           .andExpect(jsonPath("$.status").value("PENDING"));
    }
}
```

**Testcontainers** for real PostgreSQL/Kafka in integration tests.

---

## Production & Operations

### Q28. Spring Boot Actuator

| Endpoint | Purpose |
|----------|---------|
| `/actuator/health` | Liveness/readiness |
| `/actuator/metrics` | Micrometer metrics |
| `/actuator/info` | App info |
| `/actuator/env` | Environment properties |
| `/actuator/loggers` | Dynamic log levels |

**Secure in production:** expose only necessary endpoints; require authentication.

---

### Q29. Logging and observability

- **SLF4J** facade + **Logback** (default)
- Structured logging (JSON) for ELK/Datadog
- **Micrometer** → Prometheus/Grafana
- **Distributed tracing:** OpenTelemetry, Zipkin, Jaeger
- Always propagate **correlation ID** across HTTP and Kafka headers

---

## Microservices & Spring Cloud

### Q30. Common Spring Cloud components

| Component | Purpose |
|-----------|---------|
| Spring Cloud Gateway | API gateway, routing, rate limiting |
| Eureka / Consul | Service discovery |
| Config Server | Centralized configuration |
| OpenFeign | Declarative REST clients |
| Resilience4j | Circuit breaker, retry, bulkhead |
| Sleuth/Micrometer Tracing | Distributed tracing |

**Circuit breaker pattern:**
```
Closed → (failures exceed threshold) → Open → (timeout) → Half-Open → test → Closed/Open
```

Prevents cascade failures when downstream service is down.

---

# Apache Kafka

---

## Fundamentals & Architecture

### Q1. What is Apache Kafka?

A **distributed event streaming platform** that functions as a durable, append-only commit log.

**Core capabilities:**
- Publish/subscribe to streams of events
- Store streams durably and fault-tolerantly
- Process streams in real-time or replay historically

**Not just a message queue** — it's a distributed log with replay, retention, and stream processing.

---

### Q2. Kafka architecture components

```
┌─────────────┐     ┌──────────────────────────────────┐     ┌─────────────┐
│  Producers  │────▶│         Kafka Cluster            │────▶│  Consumers  │
└─────────────┘     │  ┌────────┐  ┌────────┐       │     └─────────────┘
                    │  │Broker 1│  │Broker 2│  ...  │
                    │  │ P0 P2  │  │ P1 P3  │       │
                    │  └────────┘  └────────┘       │
                    └──────────────────────────────────┘
                              ▲
                    ┌─────────┴─────────┐
                    │  KRaft Controllers │  (metadata quorum)
                    └───────────────────┘
```

| Component | Role |
|-----------|------|
| **Broker** | Server storing topic data |
| **Topic** | Named category/stream of records |
| **Partition** | Ordered, immutable sequence of records |
| **Replica** | Copy of partition for fault tolerance |
| **Producer** | Publishes records to topics |
| **Consumer** | Reads records from topics |
| **Consumer Group** | Set of consumers sharing work |
| **ZooKeeper / KRaft** | Cluster metadata and coordination |

---

### Q3. ZooKeeper vs KRaft

| | ZooKeeper (legacy) | KRaft (Kafka Raft) |
|---|-------------------|-------------------|
| Status | Deprecated in 3.x | Default in Kafka 4.0+ |
| Metadata | External ZK ensemble | Internal Raft quorum |
| Ops complexity | Manage ZK + Kafka | Kafka only |
| Scalability | ZK bottleneck at scale | Improved metadata handling |

**Interview answer:** Modern deployments use **KRaft mode** — Kafka manages its own metadata via a Raft-based controller quorum.

---

## Topics, Partitions & Offsets

### Q4. Topic vs Partition vs Offset

| Concept | Description |
|---------|-------------|
| **Topic** | Logical name (e.g., `order-events`) |
| **Partition** | Physical split; unit of parallelism and ordering |
| **Offset** | Monotonically increasing ID within a partition (0, 1, 2, ...) |
| **Record** | Key + Value + Timestamp + Headers |

**Ordering guarantee:** Only **within a single partition**, not across partitions or topics.

---

### Q5. How are partitions assigned to brokers?

- Each partition has one **leader** replica on one broker
- Follower replicas on other brokers replicate from leader
- Producers/consumers talk to **partition leaders**
- Partition count fixed at topic creation (can only increase, not decrease)

---

### Q6. How to choose partition count?

**Factors:**
- Target throughput (more partitions = more parallelism)
- Number of consumer instances in a group (max useful consumers ≈ partition count)
- Key distribution (hot partitions if skewed keys)
- End-to-end ordering requirements

**Guidelines:**
```
partitions >= max(consumer_instances, producer_throughput_target)
```

**Caution:** Too many partitions → more file handles, longer leader election, heavier rebalances.

---

### Q7. Message keys and partitioning

```java
// Default partitioner (Java client)
if (key != null) {
    partition = hash(key) % numPartitions;
} else {
    partition = sticky / round-robin;
}
```

**Same key → same partition → ordering per key.**

Example: Use `orderId` as key so all events for one order are ordered.

---

## Producers

### Q8. Producer send flow (internals)

```
Serializer(key) + Serializer(value)
  → Partitioner selects partition
  → RecordAccumulator batches per partition
  → Sender thread sends batches to broker
  → Compression (snappy, lz4, zstd, gzip)
  → Broker appends to partition log
  → Acknowledgment based on acks config
```

---

### Q9. Critical producer configurations

| Config | Values | Meaning |
|--------|--------|---------|
| `acks` | `0` | Fire-and-forget; no guarantee |
| | `1` | Leader ack; risk if leader dies before replication |
| | `all` / `-1` | All ISR replicas ack; strongest |
| `retries` | integer | Retry on transient failures |
| `enable.idempotence` | `true` | Exactly-once per producer (PID + sequence) |
| `max.in.flight.requests.per.connection` | `1-5` | Pipelines requests; with idempotence, safe up to 5 |
| `compression.type` | `lz4`, `zstd` | Reduce network/disk |
| `linger.ms` | e.g. `5` | Batch longer for throughput |
| `batch.size` | bytes | Max batch size |

**Durability recipe:** `acks=all` + `min.insync.replicas=2` + `replication.factor=3`

---

### Q10. Idempotent producer

Prevents duplicate writes from producer retries within a session.

- Producer gets **Producer ID (PID)**
- Each partition has monotonic **sequence number**
- Broker deduplicates

```properties
enable.idempotence=true
# implicitly sets: acks=all, retries=MAX, max.in.flight=5
```

---

### Q11. Transactional producer

Enables **exactly-once** across multiple partitions and consume-transform-produce flows.

```java
producer.initTransactions();
try {
    producer.beginTransaction();
    producer.send(record1);
    producer.send(record2);
    producer.commitTransaction();
} catch (Exception e) {
    producer.abortTransaction();
}
```

Requires `transactional.id` config. Used with Kafka Streams and transactional consumers.

---

## Consumers & Consumer Groups

### Q12. Consumer group mechanics

```
Topic: orders (4 partitions)

Consumer Group "order-service":
  Consumer-1 → P0, P1
  Consumer-2 → P2, P3

Consumer Group "analytics":
  Consumer-A → P0, P1, P2, P3  (independent offset)
```

**Rules:**
- Each partition consumed by **at most one** consumer per group
- If consumers > partitions, extras are idle
- Different groups read independently (pub/sub + queue hybrid)

---

### Q13. Offset management

| Config | Behavior |
|--------|----------|
| `enable.auto.commit=true` | Auto commit at interval (at-least-once risk on crash) |
| `enable.auto.commit=false` | Manual `commitSync()` / `commitAsync()` |
| `auto.offset.reset=earliest` | Start from beginning if no offset |
| `auto.offset.reset=latest` | Start from end (only new messages) |

**Offset storage:** Internal `__consumer_offsets` compacted topic.

**Manual commit pattern (at-least-once):**
```java
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, String> record : records) {
        process(record);           // 1. process
    }
    consumer.commitSync();         // 2. then commit
}
```

---

### Q14. Consumer rebalance

**Triggers:**
- Consumer joins/leaves group
- Partition count changes
- `session.timeout.ms` exceeded (consumer considered dead)
- `max.poll.interval.ms` exceeded (processing too slow)

**Assignors:**
| Assignor | Behavior |
|----------|----------|
| Range | Divide partitions by topic ranges |
| RoundRobin | Distribute evenly across all topics |
| Sticky | Minimize partition movement |
| Cooperative Sticky | Incremental rebalance (less stop-the-world) |

**Modern best practice:** CooperativeStickyAssignor — consumers don't lose all partitions during rebalance.

---

### Q15. Consumer lag

```
Lag = Latest Offset (high watermark) - Committed Consumer Offset
```

**High lag causes:**
- Slow processing logic
- Too few consumers
- Downstream bottleneck (DB)
- GC pauses / thread starvation

**Monitoring:** Kafka CLI, Burrow, Prometheus kafka_exporter, Confluent metrics.

---

### Q16. `max.poll.records` and `max.poll.interval.ms`

| Config | Risk if misconfigured |
|--------|----------------------|
| `max.poll.records` too high | Processing exceeds `max.poll.interval.ms` → rebalance |
| `max.poll.interval.ms` too low | Rebalance during long processing |
| `session.timeout.ms` too low | False positive consumer failure |

**Pattern for long processing:** pause consumption, process batch, commit, resume.

---

## Delivery Semantics

### Q17. At-most-once, at-least-once, exactly-once

| Semantic | Strategy | Trade-off |
|----------|----------|-----------|
| **At-most-once** | Commit offset before processing | May lose messages |
| **At-least-once** | Process then commit offset | May duplicate; **most common** |
| **Exactly-once** | Transactions + idempotent producer + transactional consumer | Complex; performance cost |

**Practical approach:** Design **idempotent consumers**:

```java
@Transactional
public void handle(OrderEvent event) {
    if (processedEventRepository.exists(event.getEventId())) {
        return; // already processed
    }
    orderService.apply(event);
    processedEventRepository.save(new ProcessedEvent(event.getEventId()));
}
```

Alternative: natural idempotency via unique DB constraints (`UPSERT`, `ON CONFLICT`).

---

### Q18. Exactly-once semantics (EOS) in Kafka

**Requires:**
1. Idempotent producer
2. Transactional producer (`transactional.id`)
3. `read_committed` isolation level on consumer
4. Kafka Streams or custom transactional consume-process-produce

**Use when:** financial transactions, billing, inventory where duplicates are unacceptable.

---

## Replication & Durability

### Q19. Replication and ISR

| Term | Meaning |
|------|---------|
| **Leader** | Broker serving reads/writes for partition |
| **Follower** | Replicates from leader |
| **ISR** | In-Sync Replicas — caught up within `replica.lag.time.max.ms` |
| **AR** | All assigned replicas |

**Write path with `acks=all`:**
1. Producer sends to leader
2. Leader waits for all ISR followers to replicate
3. Ack sent to producer

**Unclean leader election:** If enabled, non-ISR replica can become leader → **data loss**. Keep disabled (`unclean.leader.election.enable=false`) for durability.

---

### Q20. `min.insync.replicas`

```properties
replication.factor=3
min.insync.replicas=2
```

Producer with `acks=all` succeeds only if at least 2 replicas (including leader) acknowledge.

If ISR shrinks below `min.insync.replicas`, produce requests fail — **prefer failing over losing data**.

---

### Q21. What happens when a broker fails?

1. Controller detects broker failure
2. Elects new leader from ISR for affected partitions
3. Producers/consumers metadata refresh → redirect to new leaders
4. Under-replicated partitions until followers catch up

**Availability vs durability trade-off** controlled by replication factor, ISR, acks, and election policies.

---

## Retention & Compaction

### Q22. Log retention policies

| Policy | Config | Behavior |
|--------|--------|----------|
| Time-based | `retention.ms` | Delete segments older than X |
| Size-based | `retention.bytes` | Delete oldest when size exceeded |
| Compaction | `cleanup.policy=compact` | Keep latest record per key |

**Delete (default):** Event streams, audit logs with TTL.

**Compaction:** Changelog topics (KTables, DB CDC, config state). Tombstone records (null value) delete keys.

---

## Kafka vs Alternatives

### Q23. Kafka vs RabbitMQ

| Aspect | Kafka | RabbitMQ |
|--------|-------|----------|
| Model | Distributed commit log | Traditional message broker |
| Message lifecycle | Retained per policy; replayable | Deleted after ack |
| Throughput | Very high | Moderate |
| Ordering | Per partition | Per queue |
| Routing | Topic/partition | Exchanges, bindings, routing keys |
| Use case | Event streaming, analytics, CDC | Task queues, complex routing, RPC |
| Consumer model | Pull (poll) | Push (mostly) |

---

### Q24. Kafka vs SQS / cloud queues

| | Kafka | SQS |
|---|-------|-----|
| Ops | Self-managed or managed (MSK, Confluent) | Fully managed |
| Replay | Yes | No (standard); partial (FIFO dedup) |
| Ordering | Per partition | FIFO queues only |
| Throughput | Massive | High but different model |

---

## Spring Kafka Integration

### Q25. Spring Kafka setup

**Dependency:**
```xml
<dependency>
  <groupId>org.springframework.kafka</groupId>
  <artifactId>spring-kafka</artifactId>
</dependency>
```

**Configuration:**
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: order-service
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: com.example.events
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      properties:
        enable.idempotence: true
```

---

### Q26. Producing with `KafkaTemplate`

```java
@Service
@RequiredArgsConstructor
public class OrderEventPublisher {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public void publish(OrderEvent event) {
        kafkaTemplate.send("order-events", event.orderId().toString(), event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish {}", event.orderId(), ex);
                }
            });
    }
}
```

---

### Q27. Consuming with `@KafkaListener`

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventConsumer {
    private final OrderService orderService;

    @KafkaListener(
        topics = "order-events",
        groupId = "order-service",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
        @Payload OrderEvent event,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("partition={}, offset={}, event={}", partition, offset, event);
        orderService.handle(event);
    }
}
```

---

### Q28. Error handling and Dead Letter Topic (DLT)

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory(
    ConsumerFactory<String, OrderEvent> consumerFactory,
    KafkaTemplate<String, OrderEvent> kafkaTemplate
) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderEvent>();
    factory.setConsumerFactory(consumerFactory);

  var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
      (record, ex) -> new TopicPartition("order-events.DLT", record.partition()));

  var errorHandler = new DefaultErrorHandler(recoverer,
      new FixedBackOff(1000L, 3)); // 3 retries, 1s apart

  factory.setCommonErrorHandler(errorHandler);
  return factory;
}
```

**Never infinite-retry poison messages** — route to DLT for manual inspection.

---

### Q29. Serialization formats

| Format | Pros | Cons |
|--------|------|------|
| JSON | Human-readable, easy | Schema evolution weak, larger payload |
| Avro | Compact, Schema Registry | Requires schema management |
| Protobuf | Compact, fast, strong typing | Code generation |
| String | Simple | No structure |

**Schema Registry** (Confluent):
- Central schema store
- Compatibility modes: BACKWARD, FORWARD, FULL, NONE
- Consumers validate against registered schema

---

### Q30. Testing Kafka in Spring Boot

```java
@SpringBootTest
@Testcontainers
class OrderEventIntegrationTest {
    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
}
```

Alternatives: `@EmbeddedKafka` (lighter, less realistic).

---

## System Design Patterns

### Q31. Event-driven architecture patterns

| Pattern | Description |
|---------|-------------|
| **Event notification** | Lightweight signal; consumer fetches details |
| **Event-carried state transfer** | Full data in event; reduces chattiness |
| **Event sourcing** | State = sequence of events; Kafka as event store |
| **CQRS** | Separate read/write models; events sync views |
| **Saga** | Distributed transaction via choreographed events |

---

### Q32. Transactional Outbox pattern (critical for interviews)

**Problem:** Dual write — save to DB and publish to Kafka are not atomic.

```
Service                    Kafka
  │                          │
  ├── save Order ──────────▶ │  (can fail independently)
  └── publish Event ───────▶ │
```

**Solution:**

```
┌─────────────────────────────────────┐
│         Same DB Transaction         │
│  1. INSERT INTO orders ...          │
│  2. INSERT INTO outbox (event) ...  │
└─────────────────────────────────────┘
              │
    Outbox Relay (Debezium / polling)
              ▼
         Kafka Topic
```

**Implementations:**
- Polling publisher (read outbox table, publish, mark sent)
- **Debezium CDC** (reads DB transaction log → Kafka)
- **Transactional outbox** libraries (e.g., Axon, custom)

---

### Q33. Inbox pattern (idempotent consumption)

Store processed message IDs in inbox table; skip duplicates on redelivery.

```
Consumer receives event
  → BEGIN TX
  → IF inbox.contains(messageId) → COMMIT (skip)
  → ELSE process + insert inbox
  → COMMIT
```

Pairs with at-least-once delivery for effectively-once processing.

---

### Q34. CQRS with Kafka

```
Command → Write Service → DB (write model)
                       → Event → Kafka
                                    ↓
                              Read Service → Materialized View (read model)
```

Enables optimized read models (Elasticsearch, Redis) decoupled from write DB.

---

### Q35. Saga pattern (choreography vs orchestration)

**Choreography (Kafka events):**
```
OrderCreated → PaymentProcessed → InventoryReserved → OrderConfirmed
     ↑ each service listens and publishes next event
```

**Orchestration (central coordinator):**
```
Saga Orchestrator → commands → services → replies
```

| | Choreography | Orchestration |
|---|--------------|---------------|
| Coupling | Loose | Central dependency |
| Visibility | Harder to trace | Clear flow |
| Complexity | Many topics | Orchestrator logic |

**Compensating transactions** for rollback: `PaymentFailed` → `OrderCancelled`.

---

### Q36. Handling ordering in distributed systems

1. **Partition by entity key** (orderId, userId)
2. **Single consumer per partition**
3. Accept cross-entity ordering is not guaranteed
4. Use versioning/timestamps for stale event detection

---

### Q37. Schema evolution

**Backward compatible:** New consumers read old data (add optional fields with defaults).

**Forward compatible:** Old consumers read new data (only add fields, don't remove).

**Avro with Schema Registry** enforces compatibility on registration.

---

# Cross-Topic Integration

## Common combined interview scenarios

### Scenario 1: Place order API → downstream processing

```
Client → Spring REST Controller
       → @Transactional OrderService (save DB)
       → Outbox table (same TX)
       → Debezium/Relay → Kafka "order-events"
       → Inventory Service (@KafkaListener)
       → Payment Service (@KafkaListener)
```

**Be ready to explain:** transaction boundaries, outbox, idempotent consumers, correlation IDs.

---

### Scenario 2: Debugging high consumer lag

1. Check `max.poll.interval.ms` vs processing time
2. Profile consumer logic (DB slow queries?)
3. Scale consumers (≤ partition count)
4. Increase partitions (plan ahead — can't decrease)
5. Check rebalance storms (cooperative assignor)
6. Review batch size and parallelism

---

### Scenario 3: Exactly-once order processing

1. DB unique constraint on `event_id`
2. Inbox table pattern
3. Or Kafka transactional consume-process-produce
4. Idempotent producer for outbound events

---

### Scenario 4: Spring `@Transactional` + Kafka — antipattern

```java
@Transactional
public void handle(Event e) {
    db.save(e);
    kafkaTemplate.send("topic", e); // NOT in same transaction!
}
```

Kafka send is **not** part of DB transaction. Use outbox pattern.

---

## Observability checklist

| Signal | Java/Spring | Kafka |
|--------|-------------|-------|
| Logs | SLF4J + correlation ID | Log partition/offset/key |
| Metrics | Micrometer, JVM metrics | Consumer lag, throughput |
| Traces | OpenTelemetry | Propagate trace context in headers |
| Health | Actuator `/health` | Consumer group status |

---

# Mock Interview Cheat Sheet

## One-liner answers

| Question | Answer |
|----------|--------|
| HashMap thread-safe? | No → `ConcurrentHashMap` |
| Default bean scope? | Singleton |
| `@Transactional` rollback? | RuntimeException + Error (not checked by default) |
| Kafka ordering? | Per partition only |
| Max consumers in group? | ≤ partition count |
| Strongest producer durability? | `acks=all` + `min.insync.replicas=2` |
| Most common delivery? | At-least-once + idempotent consumer |
| N+1 fix? | JOIN FETCH / `@EntityGraph` |
| Dual write problem? | Transactional outbox |
| Self-invocation + `@Transactional`? | Proxy bypassed — extract bean |

---

## Trade-off quick reference

| Decision | Option A | Option B |
|----------|----------|----------|
| Sync vs async | REST (simple, coupled) | Kafka (decoupled, eventual) |
| JSON vs Avro | Easy dev | Production schema evolution |
| Lazy vs Eager JPA | Performance default | Eager only when needed |
| Auto vs manual offset commit | Simple | Safer processing guarantees |
| Monolith vs microservices | Faster iteration | Independent scale/deploy |

---

# 3-Day Study Plan

## Day 1 — Java (4–6 hours)

| Block | Topics | Practice |
|-------|--------|----------|
| Morning | OOP, equals/hashCode, String, collections | Explain HashMap put flow on whiteboard |
| Afternoon | Concurrency: synchronized, volatile, thread pools | Design thread-safe cache |
| Evening | JVM memory, GC, streams, Optional | Review 5 common pitfalls |

**Self-test:** Explain pass-by-value with a `List` example. Draw HashMap bucket collision resolution.

---

## Day 2 — Spring Boot (4–6 hours)

| Block | Topics | Practice |
|-------|--------|----------|
| Morning | IoC/DI, bean lifecycle, auto-config | Draw bean lifecycle diagram |
| Afternoon | REST, validation, exception handling, `@Transactional` | Explain self-invocation trap |
| Evening | JPA: relationships, N+1, lazy loading | Write JOIN FETCH query |

**Self-test:** Trace HTTP request from Tomcat to DB and back. List 5 `@ConditionalOn*` annotations.

---

## Day 3 — Kafka + Integration (4–6 hours)

| Block | Topics | Practice |
|-------|--------|----------|
| Morning | Architecture, partitions, consumer groups, offsets | Design topic with partition strategy |
| Afternoon | Delivery semantics, replication, producer acks | Compare at-least-once vs exactly-once |
| Evening | Spring Kafka, outbox, DLT, schema evolution | Whiteboard order-event flow end-to-end |

**Self-test:** Explain rebalance. Design outbox pattern for order service.

---

## Interview day (30 min review)

- [ ] SOLID + one design pattern example
- [ ] `@Transactional` propagation and rollback rules
- [ ] Kafka ordering + consumer group rules
- [ ] Outbox pattern diagram
- [ ] One debugging story
- [ ] One "I designed X with Kafka" story

---

# Further Reading

| Topic | Resource |
|-------|----------|
| Java Concurrency | *Java Concurrency in Practice* (Goetz) |
| Effective Java | *Effective Java* 3rd Ed (Joshua Bloch) |
| Spring Boot | [spring.io/guides](https://spring.io/guides) |
| Spring Kafka | [Spring for Apache Kafka docs](https://docs.spring.io/spring-kafka/reference/) |
| Kafka internals | [Kafka documentation](https://kafka.apache.org/documentation/) |
| System design | *Designing Data-Intensive Applications* (Kleppmann) |
| Outbox pattern | [microservices.io - Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html) |

---

*Last updated: June 2025 — covers Java 17+, Spring Boot 3.x, Kafka 3.x/4.x (KRaft).*
