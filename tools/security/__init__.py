"""Repository security tooling.

Modules here are linted and type-checked exactly like library code rather than
being exempted as loose scripts, because they implement controls the project
relies on: `install_gitleaks.sh` fetches the pinned scanner with checksum
verification, and `gitleaks_selftest` supplies SEC-11's negative proof that the
scanner actually fails a build.
"""
