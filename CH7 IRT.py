def intertenant_resource_trading(D, S, p, m):
    """
    演算法 2：租戶間資源交易 (IRT)
    :param D: 每個租戶的需求向量列表
    :param S: 每個租戶的初始份額向量列表
    :param p: 資源類型的數量
    :param m: 租戶的數量
    :return: 所有租戶更新後的份額向量
    """
    # 初始化變數
    S_prime = S[:]  # 當前分配的份額向量
    C = [0] * m  # 每個租戶的貢獻量
    L = [0] * m  # 每個租戶在所有資源類型上的總貢獻
    Y = 0  # 剩餘需要重新分配的資源量

    # 第一步：處理每種類型的資源
    for k in range(p):
        # 為每個租戶分配初始份額並計算貢獻量
        for i in range(m):
            S_prime[i][k] = S[i][k]  # 將初始份額賦值給當前分配向量
            U_k = D[i][k] / S[i][k] if S[i][k] > 0 else 0  # 標準化需求
            if S[i][k] > D[i][k]:  # 如果初始份額大於需求
                C[i] = S[i][k] - D[i][k]  # 計算租戶的貢獻量
            L[i] += C[i]  # 累加到租戶的總貢獻

    # 輸出當前資源分配結果（僅供測試用）
    print("當前分配份額：", S_prime)
    print("租戶貢獻量：", C)
    print("租戶總貢獻：", L)

    return S_prime

# 示例輸入數據
D = [[6, 3], [8, 1], [4, 8]]  # 每個租戶的需求向量
S = [[10, 5], [12, 4], [8, 10]]  # 每個租戶的初始份額向量
p = 2  # 資源類型數量 (CPU 和 RAM)
m = 3  # 租戶數量

# 執行演算法
updated_shares = intertenant_resource_trading(D, S, p, m)
print("更新後的資源分配：", updated_shares)
