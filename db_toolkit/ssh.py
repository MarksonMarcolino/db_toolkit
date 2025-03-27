from sshtunnel import SSHTunnelForwarder

def create_ssh_tunnel(ssh_host, ssh_port, ssh_username, ssh_private_key, remote_bind_address, local_bind_address=('localhost', 0)):
    """
    Create and start an SSH tunnel for secure remote database access.

    Parameters
    ----------
    ssh_host : str
        The hostname or IP address of the SSH server.
    ssh_port : int
        The port on which the SSH server is listening.
    ssh_username : str
        The username to authenticate with the SSH server.
    ssh_private_key : str
        Path to the private key file for SSH authentication.
    remote_bind_address : tuple of (str, int)
        The remote host and port to bind (e.g., database host and port).
    local_bind_address : tuple of (str, int), optional
        The local address and port to bind. Default is ('localhost', 0),
        which means a random free port will be used on localhost.

    Returns
    -------
    sshtunnel.SSHTunnelForwarder
        An active SSH tunnel object. Remember to call `tunnel.stop()` when done.
    """
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_username,
        ssh_private_key=ssh_private_key,
        remote_bind_address=remote_bind_address,
        local_bind_address=local_bind_address
    )
    tunnel.start()
    return tunnel