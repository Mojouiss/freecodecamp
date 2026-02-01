# Security Notice

## Security Vulnerabilities Fixed

### PyMySQL SQL Injection Vulnerability (CVE)

**Date Fixed**: 2026-02-01  
**Severity**: High  
**Affected Versions**: pymysql < 1.1.1  
**Fixed Version**: pymysql 1.1.1

#### Description
A SQL injection vulnerability was discovered in PyMySQL versions prior to 1.1.1. This vulnerability could potentially allow attackers to execute arbitrary SQL commands.

#### Impact
The financial-snapshot-library uses PyMySQL as the MySQL database driver. While the library uses SQLAlchemy's parameterized queries which provide protection against SQL injection, updating to the patched version eliminates any potential risk at the driver level.

#### Resolution
Updated pymysql dependency from 1.1.0 to 1.1.1 in:
- `requirements.txt`
- `setup.py`

#### Verification
- All 54 unit tests pass with the updated dependency
- No breaking changes introduced
- Full backward compatibility maintained

#### Recommendation
All users should update to the latest version immediately by running:
```bash
pip install --upgrade pymysql==1.1.1
```

Or reinstall the library:
```bash
pip install -r requirements.txt --upgrade
```

## Security Best Practices

The financial-snapshot-library follows security best practices:

1. **Parameterized Queries**: Uses SQLAlchemy's parameterized queries to prevent SQL injection
2. **No Hardcoded Credentials**: All credentials come from configuration
3. **Dependency Updates**: Regular monitoring and updates of dependencies
4. **Input Validation**: Validates all financial position records before processing
5. **Connection Security**: Supports SSL/TLS for Kafka and database connections

## Reporting Security Issues

If you discover a security vulnerability, please report it responsibly:

1. **Do not** open a public issue
2. Email: security@freecodecamp.org
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if available)

We will respond within 48 hours and work on a fix immediately.

## Security Update History

| Date | Vulnerability | Component | Old Version | New Version | Severity |
|------|---------------|-----------|-------------|-------------|----------|
| 2026-02-01 | SQL Injection | pymysql | 1.1.0 | 1.1.1 | High |

## Stay Updated

To stay informed about security updates:
1. Watch this repository for releases
2. Subscribe to security advisories
3. Regularly run `pip list --outdated` to check for updates
4. Use tools like `safety` or `pip-audit` to scan for vulnerabilities

## Additional Resources

- [PyMySQL Security Advisory](https://github.com/PyMySQL/PyMySQL/security/advisories)
- [OWASP SQL Injection Prevention](https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html)
- [Python Security Best Practices](https://python.readthedocs.io/en/latest/library/security_warnings.html)
