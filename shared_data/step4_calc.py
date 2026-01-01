"""
第四步：单平台计算（修复版）
功能：1. 币安时间滚动 2. 费率周期 3. 倒计时
修正：时间字段直接保留Step3的字符串，不再重复转换
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from collections import defaultdict
import time

logger = logging.getLogger(__name__)

@dataclass
class PlatformData:
    """单平台计算后的数据结构"""
    symbol: str
    exchange: str
    contract_name: str
    
    # 价格和费率
    latest_price: Optional[str] = None
    funding_rate: Optional[str] = None
    
    # 时间字段（直接保留Step3的字符串格式）
    last_settlement_time: Optional[str] = None      # 字符串格式
    current_settlement_time: Optional[str] = None
    next_settlement_time: Optional[str] = None
    
    # 时间戳备份（仅用于倒计时计算）
    last_settlement_ts: Optional[int] = None
    current_settlement_ts: Optional[int] = None
    next_settlement_ts: Optional[int] = None
    
    # 计算结果
    period_seconds: Optional[int] = None
    countdown_seconds: Optional[int] = None

class Step4Calc:
    """第四步：单平台计算"""
    
    def __init__(self):
        self.binance_cache = {}
        self.stats = defaultdict(int)
        # 新增：频率控制相关
        self.last_log_time = 0
        self.log_interval = 600  # 10分钟 = 600秒
        logger.info(f"✅ Step4Calc 初始化成功（日志频率：10分钟/次）")
    
    def _should_log(self) -> bool:
        """检查是否应该输出日志（频率控制）"""
        current_time = time.time()
        if current_time - self.last_log_time >= self.log_interval:
            self.last_log_time = current_time
            return True
        return False
    
    def process(self, aligned_results: List) -> List[PlatformData]:
        """
        处理Step3的对齐数据
        """
        # 新增：运行状态日志（带频率控制）
        if self._should_log():
            logger.info(f"🚀 Step4Calc 开始处理 {len(aligned_results)} 个合约...")
        
        results = []
        for item in aligned_results:
            try:
                okx_data = self._calc_okx(item)
                binance_data = self._calc_binance(item)
                
                if okx_data:
                    results.append(okx_data)
                if binance_data:
                    results.append(binance_data)
                
            except Exception as e:
                logger.error(f"计算失败: {item.symbol} - {e}")
                continue
        
        # 新增：完成状态日志（带频率控制）
        if self._should_log():
            logger.info(f"✅ Step4Calc 处理完成: {len(results)} 条单平台数据")
            logger.info(f"📈 Step4Calc 币安时间滚动统计: {dict(self.stats)}")
        
        return results
    
    def _calc_okx(self, aligned_item) -> Optional[PlatformData]:
        """计算OKX数据"""
        
        if not aligned_item.okx_current_ts:
            logger.debug(f"OKX {aligned_item.symbol} 无有效时间戳，跳过")
            return None
        
        # 直接保留Step3的字符串时间
        data = PlatformData(
            symbol=aligned_item.symbol,
            exchange="okx",
            contract_name=aligned_item.okx_contract_name or "",
            latest_price=aligned_item.okx_price,
            funding_rate=aligned_item.okx_funding_rate,
            current_settlement_time=aligned_item.okx_current_settlement,
            next_settlement_time=aligned_item.okx_next_settlement,
            # 保存时间戳用于倒计时计算
            current_settlement_ts=aligned_item.okx_current_ts,
            next_settlement_ts=aligned_item.okx_next_ts
        )
        
        # 计算费率周期
        if data.current_settlement_ts and data.next_settlement_ts:
            data.period_seconds = (data.next_settlement_ts - data.current_settlement_ts) // 1000
        
        # 计算倒计时
        data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
        
        return data
    
    def _calc_binance(self, aligned_item) -> Optional[PlatformData]:
        """计算币安数据（时间滚动）"""
        
        if not aligned_item.binance_current_ts:
            logger.debug(f"币安 {aligned_item.symbol} 无有效时间戳，跳过")
            return None
        
        symbol = aligned_item.symbol
        
        # 初始化缓存
        if symbol not in self.binance_cache:
            self.binance_cache[symbol] = {
                "last_ts": aligned_item.binance_last_ts,
                "current_ts": aligned_item.binance_current_ts
            }
        
        cache = self.binance_cache[symbol]
        T1 = cache["last_ts"]
        T2 = cache["current_ts"]
        T3 = aligned_item.binance_current_ts
        
        # 时间滚动逻辑
        if T2 and T3 != T2:
            logger.info(f"币安 {symbol} 结算时间更新: T1={T2} → T2={T3}")
            T1 = T2
            T2 = T3
            cache["last_ts"] = T1
            cache["current_ts"] = T2
            self.stats["binance_rollovers"] += 1
        
        self.stats["binance_updates"] += 1
        
        # 构建数据（保留字符串，保存时间戳用于计算）
        data = PlatformData(
            symbol=symbol,
            exchange="binance",
            contract_name=aligned_item.binance_contract_name or "",
            latest_price=aligned_item.binance_price,
            funding_rate=aligned_item.binance_funding_rate,
            last_settlement_time=aligned_item.binance_last_settlement,  # 字符串！
            current_settlement_time=aligned_item.binance_current_settlement,
            next_settlement_time=aligned_item.binance_next_settlement,
            last_settlement_ts=T1,
            current_settlement_ts=T2
        )
        
        # 计算费率周期
        if data.current_settlement_ts and data.last_settlement_ts:
            data.period_seconds = (data.current_settlement_ts - data.last_settlement_ts) // 1000
        
        # 计算倒计时
        data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
        
        return data
    
    def _calc_countdown(self, settlement_ts: Optional[int]) -> Optional[int]:
        """计算倒计时"""
        if not settlement_ts:
            return None
        
        try:
            now_ms = int(time.time() * 1000)
            return max(0, (settlement_ts - now_ms) // 1000)
        except Exception as e:
            logger.warning(f"倒计时计算失败: {settlement_ts} - {e}")
            return None
    
    def get_cache_status(self, symbol: str) -> Dict[str, Any]:
        """查询单个合约的币安缓存状态"""
        cache = self.binance_cache.get(symbol, {})
        return {
            "has_last_ts": cache.get("last_ts") is not None,
            "has_current_ts": cache.get("current_ts") is not None,
            "last_ts": cache.get("last_ts"),
            "current_ts": cache.get("current_ts"),
            "last_settlement_time": self._ts_to_str(cache.get("last_ts")),
            "current_settlement_time": self._ts_to_str(cache.get("current_ts"))
        }
    
    def get_cache_report(self) -> Dict[str, Any]:
        """获取币安缓存状态完整报告"""
        report = {
            "total_cached": len(self.binance_cache),
            "with_last_ts": 0,
            "without_last_ts": 0,
            "symbols_without_history": [],
            "symbol_details": {}
        }
        
        for symbol, cache in self.binance_cache.items():
            if cache.get("last_ts"):
                report["with_last_ts"] += 1
            else:
                report["without_last_ts"] += 1
                report["symbols_without_history"].append(symbol)
            
            # 添加详细缓存信息
            report["symbol_details"][symbol] = {
                "last_ts": cache.get("last_ts"),
                "current_ts": cache.get("current_ts"),
                "last_settlement_time": self._ts_to_str(cache.get("last_ts")),
                "current_settlement_time": self._ts_to_str(cache.get("current_ts")),
                "status": "complete" if cache.get("last_ts") else "pending_history"
            }
        
        return report
    
    def _ts_to_str(self, ts: Optional[int]) -> Optional[str]:
        """内部辅助方法：时间戳转字符串（仅供报告使用）"""
        if ts is None or ts <= 0:
            return None
        
        try:
            from datetime import datetime, timedelta
            dt_utc = datetime.utcfromtimestamp(ts / 1000)
            dt_bj = dt_utc + timedelta(hours=8)
            return dt_bj.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return None
