def intertenant_weight_adjustment(d, s, S):
    """
    演算法 3：租戶內部權重調整 (IWA)
    :param d: 每個虛擬機的需求向量列表
    :param s: 每個虛擬機的初始份額列表
    :param S: 總份額容量
    :return: 每個虛擬機更新後的份額分配
    """
    # 初始化變數
    n = len(s)  # 虛擬機的數量
    F = 0  # 剩餘容量
    G = 0  # 未滿足需求總量
    s_prime = s[:]  # 初始化更新後的份額分配，初始值與 s 相同

    # 計算初始總份額與新分配容量之間的差異
    for j in range(n):
        if d[j] >= s[j]:  # 如果需求大於或等於分配的份額
            G += d[j] - s[j]  # 增加未滿足需求
        else:
            F += s[j] - d[j]  # 增加剩餘容量

    # 分配剩餘容量給未滿足需求的虛擬機
    for j in range(n):
        if d[j] >= s[j]:  # 如果需求大於或等於分配的份額
            s_prime[j] = s[j] + (d[j] - s[j]) * (F / G)  # 根據未滿足需求的比例重新分配
        else:
            s_prime[j] = d[j]  # 如果需求小於分配的份額，則分配其實際需求

    # 返回更新後的分配
    return s_prime


# 示例輸入數據
d = [10, 5, 15]  # 每個虛擬機的需求向量
s = [8, 6, 12]  # 每個虛擬機的初始份額
S = sum(s)  # 總份額容量

# 執行演算法
updated_shares = intertenant_weight_adjustment(d, s, S)
print("更新後的資源分配：", updated_shares)
