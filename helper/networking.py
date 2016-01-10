import ipaddress


def is_ip(ip):
    try:
        if ':' in ip:
            ip = ip.split(':')[0]
        ipaddress.ip_address(ip)
        return True
    except OSError as E:
        return False
