class Stats:
    total_actions = 0
    times_arr = []
    total_actions_time = 0
    error_counter = 0
    objects_existed  = 0
    succeeded_queries = 0
    succeeded_commands = 0

    def __init__(self):
        self.total_actions = 0
        self.times_arr = []
        self.total_actions_time = 0
        self.error_counter = 0
        self.objects_existed = 0
        self.succeeded_queries = 0
        self.succeeded_commands = 0
