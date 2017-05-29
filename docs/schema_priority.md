# Schemas and You

The purpose of this library is to help handle the check-in/check-out of noSQL schemas.  As such, the behavior is designed to "just work" for both dev and prod

# Schema Priority

1. Source-of-Truth: master MongoDB
2. Library included schemas
3. Local-override schemas

# Nitty Gritty

First, the library will try to connect to MongoDB to get latest schemas when requested.  These are then saved to a tinydb for easy on-disk validation.

Caches can be updated on a timer, to make sure live services don't end up out-of-date, but also don't need to ask the db for permission on every write.

Also, for remote development, it allows for local-override when debugging.  In this case it should stay on local-override mode (with warning)
