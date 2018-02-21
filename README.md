Important:

Although EntityManagerFactory instances are thread-safe, EntityManager instances are not.
The injected JPA EntityManager behaves like an EntityManager fetched from an application server's JNDI environment, as defined by the JPA specification.
It delegates all calls to the current transactional EntityManager, if any; otherwise, it falls back to a newly created EntityManager per operation, in effect making its usage thread-safe.
