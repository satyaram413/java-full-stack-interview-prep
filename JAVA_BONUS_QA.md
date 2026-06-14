# Java & Spring Bonus Interview Q&A

Plain-English answers for common interview questions. Each topic: **what it is → why it exists → example → interview one-liner**.

[← Back to main guide](README.md)

---

## Table of Contents

1. [Streams](#streams)
2. [Collections](#collections)
3. [Interfaces & Default Methods](#interfaces--default-methods)
4. [Exceptions](#exceptions)
5. [Jackson & Serialization](#jackson--serialization)
6. [Spring REST](#spring-rest)
7. [Localization](#localization)
8. [Enums](#enums)
9. [final, finally, finalize & try-with-resources](#final-finally-finalize--try-with-resources)
10. [Method & Constructor References](#method--constructor-references)

---

# Streams

## What are Streams and why do we need them?

**What:** A **Stream** is a sequence of elements you process in a **pipeline** — filter, transform, collect — without writing manual `for` loops.

**Why:**
- **Readable** — says *what* you want, not *how* to loop
- **Composable** — chain operations (`filter` → `map` → `sorted` → `collect`)
- **Parallelizable** — `parallelStream()` for CPU-heavy work on large data
- **No mutation** of source (ideally) — fewer bugs

```java
List<String> names = people.stream()
    .filter(p -> p.getAge() >= 18)
    .map(Person::getName)
    .sorted()
    .toList();
```

**When NOT to use:** Simple 3-line loop; I/O inside streams; you need to break/continue in complex ways; very small collections where a loop is clearer.

**Interview one-liner:** *"Streams let me process collections declaratively in a pipeline instead of imperative loops, with optional parallelism."*

---

## What does "lazy" mean for streams?

**Lazy** = intermediate operations **do not run** until a **terminal operation** triggers the pipeline.

```java
List<Integer> nums = List.of(1, 2, 3, 4, 5);

Stream<Integer> stream = nums.stream()
    .filter(n -> {
        System.out.println("filter: " + n);  // does NOT print yet
        return n % 2 == 0;
    })
    .map(n -> {
        System.out.println("map: " + n);     // does NOT print yet
        return n * 10;
    });

System.out.println("pipeline built — nothing ran above");

List<Integer> result = stream.toList();  // NOW it runs
```

**Why lazy matters:**
- **Short-circuit** — `findFirst()` can stop after first match without processing entire list
- **Fusion** — JVM can optimize chained operations
- **Efficiency** — don't do work if nobody consumes the result

**Interview one-liner:** *"Intermediate ops are lazy; nothing executes until a terminal op like collect or forEach runs."*

---

## Intermediate vs terminal operators

| Type | Role | Returns | Examples |
|------|------|---------|----------|
| **Intermediate** | Transform stream → stream | `Stream<T>` | `filter`, `map`, `flatMap`, `sorted`, `distinct`, `limit`, `skip`, `peek` |
| **Terminal** | Produces final result | Non-stream | `collect`, `toList`, `forEach`, `count`, `reduce`, `findFirst`, `anyMatch`, `min`, `max` |

```java
nums.stream()
    .filter(n -> n > 0)      // intermediate
    .map(n -> n * 2)         // intermediate
    .collect(Collectors.toList());  // terminal — triggers everything
```

---

## Difference between intermediate and terminal operators

| | Intermediate | Terminal |
|---|--------------|----------|
| **Count in pipeline** | Zero or many | Exactly **one** (required) |
| **Lazy?** | Yes | No — runs the pipeline |
| **Return type** | `Stream` | `List`, `int`, `Optional`, `void`, etc. |
| **Can chain after?** | Yes | No — stream is done |
| **Purpose** | Setup transformation | Get result or side effect |

**Rule:** `stream → intermediate* → terminal` — without terminal, nothing happens.

---

## Reusing a stream after terminal operation

A stream can only be consumed **once**. After a terminal operation, the stream is **closed**.

```java
List<Integer> numbers = List.of(1, 2, 3, 4, 5);
Stream<Integer> stream = numbers.stream();

long count = stream.count();        // terminal — stream consumed
// int sum = stream.mapToInt(i -> i).sum();  // IllegalStateException: stream has already been operated upon or closed
```

**Full example:**

```java
List<Integer> numbers = List.of(10, 20, 30);

Stream<Integer> s = numbers.stream().filter(n -> n > 15);

List<Integer> list = s.toList();   // OK — first terminal
// s.forEach(System.out::println);  // THROWS IllegalStateException

// Fix: create a new stream each time
numbers.stream().filter(n -> n > 15).forEach(System.out::println);
```

**Interview one-liner:** *"Streams are single-use; after a terminal op you must call `.stream()` again on the source."*

---

# Collections

## What are Collections?

**Collections Framework** (`java.util`) is Java's standard library for storing and manipulating groups of objects.

**Core interfaces:**

```
Iterable
  └── Collection
        ├── List      — ordered, allows duplicates
        ├── Set       — no duplicates
        └── Queue     — FIFO processing
              └── Deque — double-ended queue

Map (separate) — key-value pairs, not a Collection
```

You program to **interfaces** (`List`, `Map`) and pick implementations (`ArrayList`, `HashMap`) based on needs.

---

## List vs Set vs Map vs Queue vs Deque — when to use which?

| Type | Order | Duplicates | Access | Choose when |
|------|-------|------------|--------|-------------|
| **List** | Yes (insertion) | Yes | By index | Ordered sequence, duplicates OK — `ArrayList` default |
| **Set** | Varies | **No** | By value | Unique items — `HashSet` for speed, `TreeSet` for sorted |
| **Map** | Key-based | Unique keys | By key | Lookup by ID/key — `HashMap` default |
| **Queue** | FIFO | Yes | Head/tail | Task scheduling, BFS — `LinkedList`, `PriorityQueue` |
| **Deque** | Both ends | Yes | Front & back | Stack (`push`/`pop`), sliding window — `ArrayDeque` |

```java
List<String> todo = new ArrayList<>();           // keep order
Set<Long> seenIds = new HashSet<>();             // unique IDs
Map<Long, User> usersById = new HashMap<>();     // fast lookup
Queue<Task> pending = new LinkedList<>();        // process in order
Deque<Integer> stack = new ArrayDeque<>();       // LIFO at one end
```

---

## SortedSet vs TreeSet

**Short answer:** `TreeSet` **is** the main implementation of `SortedSet`. You rarely choose between them — you choose **`SortedSet` interface** vs other sets.

| | `SortedSet` | `TreeSet` |
|---|-------------|-----------|
| **What** | Interface — sorted, no duplicates | Concrete class implementing `SortedSet` |
| **Implementation** | — | Red-Black Tree |
| **Order** | Ascending (natural or `Comparator`) | Same |
| **Performance** | — | `add`/`contains`/`remove` = O(log n) |
| **Alternative** | — | `ConcurrentSkipListSet` for thread-safe sorted set |

```java
SortedSet<Integer> sorted = new TreeSet<>();
sorted.add(30);
sorted.add(10);
sorted.add(20);
// iteration: 10, 20, 30

SortedSet<String> byLength = new TreeSet<>(Comparator.comparingInt(String::length));
```

**`LinkedHashSet`** = unique + insertion order (not sorted). **`TreeSet`** = unique + sorted.

---

# Interfaces & Default Methods

## What are default methods and why were they introduced?

**What:** A method in an **interface** with a body (`default` keyword), Java 8+.

```java
public interface Notifier {
    void send(String msg);  // abstract — must implement

    default void log(String msg) {
        System.out.println("[LOG] " + msg);
    }
}
```

**Why introduced:**
1. **Evolve APIs without breaking old code** — add new methods to `List`, `Collection`, etc. without forcing every implementor to change
2. **Backward compatibility** — existing classes implementing the interface still compile
3. **Shared utility in interface** — optional common behavior

**Without default methods:** Adding one method to `Collection` would break every custom `Collection` implementation in the world.

**Rules:**
- Class can **override** default method
- If two interfaces have same default method → class must override (diamond problem)
- `default` is not for `Object` methods like `equals` — use normal override in class

---

# Exceptions

## Checked vs unchecked exceptions

| | Checked | Unchecked |
|---|---------|-----------|
| **Extends** | `Exception` (not `RuntimeException`) | `RuntimeException` |
| **Compile-time** | Must catch or `throws` | Optional |
| **Examples** | `IOException`, `SQLException` | `NullPointerException`, `IllegalArgumentException` |
| **Meaning** | Recoverable external failure | Programming / business rule error |

```java
// Checked — compiler forces handling
public void readFile() throws IOException {
    Files.readString(Path.of("data.txt"));
}

// Unchecked — your API design choice
public User findUser(Long id) {
    return repo.findById(id)
        .orElseThrow(() -> new UserNotFoundException(id));
}
```

**Modern practice:** Use unchecked for business exceptions; checked at true I/O boundaries or wrap in `RuntimeException`.

---

## How to create a custom exception

```java
// Unchecked — most common for business rules
public class OrderNotFoundException extends RuntimeException {
    public OrderNotFoundException(Long orderId) {
        super("Order not found: " + orderId);
    }
}

// Checked — when caller must handle
public class PaymentFailedException extends Exception {
    private final String reason;

    public PaymentFailedException(String reason) {
        super("Payment failed: " + reason);
        this.reason = reason;
    }

    public String getReason() { return reason; }
}
```

**Tips:**
- Provide meaningful message
- Constructors: no-arg, message, message + cause
- Don't overuse checked exceptions in REST services

---

## Base class for `Exception` and `Error`

```
Throwable
  ├── Exception          ← checked + unchecked (RuntimeException branch)
  │     └── RuntimeException   ← unchecked only
  └── Error              ← serious JVM problems — don't catch normally
        ├── OutOfMemoryError
        ├── StackOverflowError
        └── ...
```

| Class | Catch in app code? |
|-------|-------------------|
| `Exception` | Yes — expected failures |
| `RuntimeException` | Yes — bugs / business rules |
| `Error` | **No** (generally) — JVM is broken; log and exit |

**Interview one-liner:** *"`Throwable` is root; `Exception` is for recoverable app errors; `Error` is for serious JVM failures like OOM."*

---

# Jackson & Serialization

## `@JsonIgnore` vs `transient`

| | `@JsonIgnore` | `transient` |
|---|---------------|-------------|
| **Layer** | Jackson (JSON) only | Java serialization (`Serializable`) |
| **Scope** | Field ignored when **JSON** serializing/deserializing | Field skipped in **Java ObjectOutputStream** |
| **Works with** | REST APIs, Jackson | RMI, legacy Java serialization |
| **Inheritance** | Jackson annotations | JVM keyword on field |

```java
public class User {
    private String email;

    @JsonIgnore
    private String password;   // not in JSON response

    private transient String cache;  // not in Java binary serialization
}
```

**Spring REST uses Jackson** → use `@JsonIgnore` or `@JsonProperty(access = READ_ONLY)` for API fields.

**JPA note:** `transient` keyword is **different** from `@Transient` annotation (JPA ignores field for DB). Three different "transient" concepts — know which layer you're talking about!

| Keyword/Annotation | Layer |
|--------------------|-------|
| `transient` (field) | Java serialization |
| `@Transient` (JPA) | Database mapping |
| `@JsonIgnore` | JSON (Jackson) |

---

# Spring REST

## `@Controller` vs `@RestController`

| | `@Controller` | `@RestController` |
|---|---------------|-------------------|
| **Purpose** | MVC — return **view name** (HTML) | REST API — return **data** (JSON/XML) |
| **Equivalent to** | `@Controller` alone | `@Controller` + `@ResponseBody` on every method |
| **Return type** | `String` view name → Thymeleaf/JSP | Object → JSON via `HttpMessageConverter` |
| **Use when** | Server-rendered pages | REST APIs, microservices |

```java
@Controller
public class PageController {
    @GetMapping("/home")
    public String home(Model model) {
        return "home";  // resolves to home.html
    }
}

@RestController
@RequestMapping("/api/users")
public class UserController {
    @GetMapping("/{id}")
    public UserDto get(@PathVariable Long id) {
        return userService.get(id);  // → JSON
    }
}
```

---

## How to handle exceptions in Spring Boot

**Global handler** with `@RestControllerAdvice`:

```java
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(UserNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(UserNotFoundException ex) {
        return ResponseEntity
            .status(HttpStatus.NOT_FOUND)
            .body(new ErrorResponse("USER_NOT_FOUND", ex.getMessage()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex) {
        String details = ex.getBindingResult().getFieldErrors().stream()
            .map(e -> e.getField() + ": " + e.getDefaultMessage())
            .collect(Collectors.joining(", "));
        return ResponseEntity.badRequest()
            .body(new ErrorResponse("VALIDATION_ERROR", details));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneral(Exception ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(new ErrorResponse("INTERNAL_ERROR", "Something went wrong"));
    }
}
```

**Consistent error DTO:**

```java
public record ErrorResponse(String code, String message, Instant timestamp) {
    public ErrorResponse(String code, String message) {
        this(code, message, Instant.now());
    }
}
```

**Also know:** `@ResponseStatus` on exception class, `ProblemDetail` (Spring 6 / RFC 7807).

---

## How to implement versioning in REST API

| Strategy | Example | Pros | Cons |
|----------|---------|------|------|
| **URI path** | `/api/v1/users`, `/api/v2/users` | Obvious, easy to route | URL pollution |
| **Header** | `Accept: application/vnd.myapp.v2+json` | Clean URLs | Harder to test in browser |
| **Query param** | `/api/users?version=2` | Simple | Easy to forget |
| **Custom header** | `X-API-Version: 2` | Common in enterprises | Not self-documenting |

**Most common in interviews — URI versioning:**

```java
@RestController
@RequestMapping("/api/v1/orders")
public class OrderControllerV1 { ... }

@RestController
@RequestMapping("/api/v2/orders")
public class OrderControllerV2 { ... }
```

**Best practices:**
- Version only on **breaking** changes
- Support old version for deprecation period
- Document in OpenAPI/Swagger
- Don't version every small additive field

---

# Localization

## What is localization in Java?

**Localization (l10n)** = adapting your app to **language, region, and format** (dates, numbers, currency).

**Related:** **Internationalization (i18n)** = building the app so it *can* be localized (externalized messages).

```java
// Resource bundles — messages per locale
// messages_en.properties → greeting=Hello
// messages_fr.properties → greeting=Bonjour

ResourceBundle bundle = ResourceBundle.getBundle("messages", Locale.FRENCH);
String greeting = bundle.getString("greeting");  // "Bonjour"

// Format dates and numbers per locale
DateTimeFormatter fmt = DateTimeFormatter
    .ofLocalizedDate(FormatStyle.LONG)
    .withLocale(Locale.JAPAN);

NumberFormat currency = NumberFormat.getCurrencyInstance(Locale.US);
```

**Spring Boot:**

```java
@Bean
public LocaleResolver localeResolver() {
    SessionLocaleResolver resolver = new SessionLocaleResolver();
    resolver.setDefaultLocale(Locale.ENGLISH);
    return resolver;
}

// messages.properties, messages_de.properties
// @Autowired MessageSource messages;
// messages.getMessage("welcome", null, locale);
```

**Interview one-liner:** *"Localization adapts UI text and formats to user's locale using ResourceBundle or Spring MessageSource with locale-specific property files."*

---

# Enums

## What are enums?

An **enum** is a type with a **fixed set of constants** — days of week, order status, HTTP methods.

```java
public enum OrderStatus {
    PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED
}

OrderStatus status = OrderStatus.PENDING;
```

**Under the hood:** Enums are classes that extend `java.lang.Enum` — type-safe constants, better than `public static final int PENDING = 0`.

---

## Enum vs class

| | Enum | Class |
|---|------|-------|
| **Instances** | Fixed constants only | Unlimited `new` objects |
| **Inheritance** | Cannot extend (extends `Enum`) | Can extend one class |
| **Type safety** | `OrderStatus.PENDING` — compiler checks | Magic strings/ints |
| **Use case** | Fixed vocabulary | General objects with behavior/state |

```java
// BAD — magic strings
String status = "PENDNG";  // typo compiles

// GOOD
OrderStatus status = OrderStatus.PENDING;
```

---

## Can enums implement interfaces?

**Yes.**

```java
public interface Describable {
    String getDescription();
}

public enum Priority implements Describable {
    LOW("Low priority"),
    HIGH("High priority");

    private final String description;

    Priority(String description) {
        this.description = description;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
```

Each enum constant is an instance of the enum class, so it can implement interface methods.

---

## Convert String to Enum

```java
// Must match constant name exactly (case-sensitive by default)
OrderStatus status = OrderStatus.valueOf("PENDING");

// Safe — no exception if invalid
Optional<OrderStatus> opt = Arrays.stream(OrderStatus.values())
    .filter(s -> s.name().equalsIgnoreCase("pending"))
    .findFirst();

// Java 12+ switch
OrderStatus s = OrderStatus.valueOf(input);
```

**Invalid name** → `IllegalArgumentException`. Always validate user input.

```java
public OrderStatus parseStatus(String raw) {
    try {
        return OrderStatus.valueOf(raw.toUpperCase());
    } catch (IllegalArgumentException e) {
        throw new BadRequestException("Invalid status: " + raw);
    }
}
```

---

## Can enums have constructors?

**Yes** — must be **private** (implicitly). Used to attach data to each constant.

```java
public enum Currency {
    USD("$", "US Dollar"),
    EUR("€", "Euro"),
    INR("₹", "Indian Rupee");

    private final String symbol;
    private final String displayName;

    Currency(String symbol, String displayName) {
        this.symbol = symbol;
        this.displayName = displayName;
    }

    public String getSymbol() { return symbol; }
    public String getDisplayName() { return displayName; }
}

Currency.INR.getSymbol();  // "₹"
```

**Rules:** Constructor runs when each enum constant is created (once, at class load). Cannot `new OrderStatus()` from outside.

---

# final, finally, finalize & try-with-resources

## `final` vs `finally` vs `finalize`

| | `final` | `finally` | `finalize()` |
|---|---------|-----------|--------------|
| **What** | Keyword | Block after try/catch | Method on `Object` |
| **Purpose** | Variable can't reassign; method can't override; class can't extend | Always runs cleanup after try | GC callback before collect |
| **Status** | Use daily | Use try-with-resources instead | **Deprecated — never use** |

```java
final int MAX = 100;           // can't reassign MAX
final class Immutable {}       // can't subclass
final void method() {}         // can't override

try {
    risky();
} finally {
    cleanup();  // runs even if exception
}
```

---

## try-with-resources

Automatically closes resources that implement **`AutoCloseable`** / **`Closeable`**.

```java
// Before Java 7 — verbose
Reader reader = null;
try {
    reader = new FileReader("file.txt");
    // use reader
} finally {
    if (reader != null) reader.close();
}

// try-with-resources — preferred
try (Reader reader = new FileReader("file.txt");
     BufferedReader br = new BufferedReader(reader)) {
    return br.readLine();
}  // close() called automatically in reverse order
```

**Requirements:** Resource declared in `try (...)` parentheses. Must implement `AutoCloseable`.

**Interview one-liner:** *"try-with-resources auto-closes AutoCloseable resources, even if an exception is thrown — cleaner than manual finally."*

---

# Method & Constructor References

## Method reference

Shorthand for a **lambda that calls one existing method**.

| Syntax | Lambda equivalent |
|--------|-------------------|
| `Person::getName` | `p -> p.getName()` |
| `System.out::println` | `x -> System.out.println(x)` |
| `Integer::parseInt` | `s -> Integer.parseInt(s)` |
| `String::length` | `s -> s.length()` |

```java
List<String> names = people.stream()
    .map(Person::getName)           // instance method ref
    .map(String::toUpperCase)       // instance method on arbitrary arg
    .forEach(System.out::println);  // static/instance ref
```

**Types:** static, instance on particular object, instance on arbitrary object, constructor ref.

---

## Constructor reference

Shorthand for **lambda that creates a new object**.

| Syntax | Lambda equivalent |
|--------|-------------------|
| `ArrayList::new` | `() -> new ArrayList<>()` |
| `Person::new` | `name -> new Person(name)` |

```java
List<String> names = List.of("alice", "bob");

List<Person> people = names.stream()
    .map(Person::new)   // calls Person(String name) constructor
    .toList();

Supplier<List<String>> factory = ArrayList::new;
List<String> list = factory.get();
```

**Use when:** lambda body is only `new SomeClass(...)` — cleaner than `x -> new SomeClass(x)`.

---

## Quick interview cheat sheet

| Question | One-line answer |
|----------|-----------------|
| Why streams? | Declarative collection processing in pipelines |
| Lazy streams? | Intermediate ops run only when terminal op executes |
| Reuse stream? | No — `IllegalStateException` after terminal op |
| List vs Set vs Map? | Ordered duplicates / unique / key-value lookup |
| TreeSet? | Sorted unique set — O(log n) |
| Default methods? | Add interface methods without breaking implementors |
| Checked exception? | Must catch/declare — `IOException` |
| `@JsonIgnore` vs `transient`? | JSON vs Java serialization |
| `@RestController`? | `@Controller` + JSON on every method |
| API versioning? | Usually `/api/v1/...` URI path |
| Enum vs class? | Fixed constants vs unlimited instances |
| try-with-resources? | Auto-close `AutoCloseable` in try block |
| Method ref? | `Person::getName` ≡ `p -> p.getName()` |

[← Back to main guide](README.md)
