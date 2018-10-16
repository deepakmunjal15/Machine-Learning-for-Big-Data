class LogEntry:
    """A simple data object"""
    def __init__(self, username, remoteIp, remotePort, version, rawInput):
        self.username = username
        self.remoteIp = remoteIp
        self.remotePort = remotePort
        self.version = version
        self.rawInput = rawInput

    def __str__(self):
        return "LogEntry {\n\tusername: %s,\n\tremoteIp: %s,\n\tremotePort: %s,\n\tversion: %s,\n\trawInput: %s\n}" \
               % (self.username, self.remoteIp, self.remotePort, self.version, self.rawInput)

    def __repr__(self):
        return "LogEntry {\n\tusername: %s,\n\tremoteIp: %s,\n\tremotePort: %s,\n\tversion: %s,\n\trawInput: %s\n}" \
               % (self.username, self.remoteIp, self.remotePort, self.version, self.rawInput)