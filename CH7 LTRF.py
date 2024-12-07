class LTRF:
    def __init__(self, total_resources, client_weights):
        """
        初始化 LTRF 系統
        :param total_resources: 系統中的總資源量
        :param client_weights: 每個用戶的權重列表
        """
        self.total_resources = total_resources  # 系統總資源
        self.current_allocated = [0] * len(client_weights)  # 當前分配資源
        self.total_used = [0] * len(client_weights)  # 每個用戶已使用的總資源
        self.client_weights = client_weights  # 每個用戶的權重

    def allocate_resources(self, client_index, resource_demand):
        """
        嘗試為用戶分配資源
        :param client_index: 用戶索引
        :param resource_demand: 該任務需要的資源量
        :return: 分配是否成功
        """
        if sum(self.current_allocated) + resource_demand <= self.total_resources:
            self.current_allocated[client_index] += resource_demand
            self.total_used[client_index] += resource_demand
            return True
        return False

    def release_resources(self, client_index, resources_to_release):
        """
        從用戶釋放資源
        :param client_index: 用戶索引
        :param resources_to_release: 要釋放的資源量
        """
        self.current_allocated[client_index] -= resources_to_release

    def schedule_tasks(self, pending_tasks):
        """
        根據 LTRF 演算法調度任務
        :param pending_tasks: 每個用戶的任務資源需求列表
        """
        while any(task > 0 for task in pending_tasks):  # 當還有未完成任務
            # 找到加權資源使用量最小的用戶
            weighted_usage = [
                self.total_used[i] / self.client_weights[i]
                for i in range(len(self.client_weights))
            ]
            client_index = weighted_usage.index(min(weighted_usage))

            # 該用戶的下一個任務需求
            resource_demand = pending_tasks[client_index]

            # 嘗試分配資源
            if resource_demand > 0 and self.allocate_resources(client_index, resource_demand):
                print(f"分配 {resource_demand} 資源給用戶 {client_index + 1}")
                pending_tasks[client_index] = 0  # 任務完成
            else:
                # 若分配失敗，等待資源釋放
                print(f"等待用戶 {client_index + 1} 釋放資源...")
                # 模擬資源釋放（這裡假設釋放一半資源）
                resources_to_release = self.current_allocated[client_index] // 2
                self.release_resources(client_index, resources_to_release)
                print(f"用戶 {client_index + 1} 釋放了 {resources_to_release} 資源")

# 示例運行
total_resources = 100
client_weights = [1, 1]  # 兩個用戶的權重相同
pending_tasks = [20, 100]  # 每個用戶的資源需求

ltrf = LTRF(total_resources, client_weights)
ltrf.schedule_tasks(pending_tasks)
