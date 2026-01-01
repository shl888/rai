"""
第五步：跨平台计算 + 最终数据打包（数据计算专用版）
功能：1. 计算价格差、费率差（绝对值+百分比） 2. 打包双平台所有字段 3. 倒计时
原则：只做数据计算，不做业务判断。所有数据都保留，交给后续交易模块处理。
输出：原始套利数据，每条包含双平台完整信息
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime
import time  # 新增：用于频率控制
import traceback

logger = logging.getLogger(__name__)

@dataclass
class CrossPlatformData:
    """最终跨平台套利数据结构"""
    symbol: str
    
    # 计算字段（没有默认值，放前面）
    price_diff: float              # |OKX价格 - 币安价格|（绝对值）
    price_diff_percent: float      # 价格百分比差（以低价为准）
    rate_diff: float               # |OKX费率 - 币安费率|
    
    # 必须先放没有默认值的字段！
    okx_price: str
    okx_funding_rate: str
    binance_price: str
    binance_funding_rate: str
    
    # 再放有默认值的字段
    okx_period_seconds: Optional[int] = None
    okx_countdown_seconds: Optional[int] = None
    okx_last_settlement: Optional[str] = None
    okx_current_settlement: Optional[str] = None
    okx_next_settlement: Optional[str] = None
    
    binance_period_seconds: Optional[int] = None
    binance_countdown_seconds: Optional[int] = None
    binance_last_settlement: Optional[str] = None
    binance_current_settlement: Optional[str] = None
    binance_next_settlement: Optional[str] = None
    
    # 数据源标记（不含业务判断）
    metadata: Dict[str, Any] = field(default_factory=lambda: {
        "calculated_at": None,
        "price_validation": "unknown",
        "rate_validation": "unknown",
        "source": "step5_cross_calc"
    })
    
    def __post_init__(self):
        """只做标记，不做过滤"""
        self.metadata["calculated_at"] = datetime.now().isoformat()
        
        # 标记数据状态（仅标记，不过滤）
        try:
            okx_price = float(self.okx_price)
            binance_price = float(self.binance_price)
            self.metadata["price_validation"] = "valid" if okx_price > 0 and binance_price > 0 else "invalid"
        except:
            self.metadata["price_validation"] = "error"

class Step5CrossCalc:
    """第五步：跨平台计算（专注数据计算版）"""
    
    def __init__(self):
        # 基本统计（不包含业务逻辑）
        self.stats = {
            "total_processed": 0,
            "successful": 0,
            "failed": 0,
            "okx_missing": 0,
            "binance_missing": 0,
            "price_invalid": 0,
            "calc_errors": 0,
            "price_too_low": 0,  # 新增：价格过低统计
            "start_time": None,
            "end_time": None
        }
        # 新增：频率控制相关
        self.last_log_time = 0
        self.log_interval = 600  # 10分钟 = 600秒
        logger.info(f"✅ Step5CrossCalc 初始化成功（日志频率：10分钟/次）")
    
    def _should_log(self) -> bool:
        """检查是否应该输出日志（频率控制）"""
        current_time = time.time()
        if current_time - self.last_log_time >= self.log_interval:
            self.last_log_time = current_time
            return True
        return False
    
    def process(self, platform_results: List) -> List[CrossPlatformData]:
        """
        处理Step4的单平台数据，只做数据计算，不做业务过滤
        """
        self.stats["start_time"] = datetime.now().isoformat()
        self.stats["total_processed"] = len(platform_results)
        
        # 新增：运行状态日志（带频率控制）
        if self._should_log():
            logger.info(f"🚀 Step5CrossCalc 开始处理 {len(platform_results)} 条单平台数据...")
        
        if not platform_results:
            logger.warning("⚠️ 输入数据为空")
            return []
        
        # 按symbol分组
        grouped = defaultdict(list)
        for item in platform_results:
            # 只检查基本格式，不判断业务合理性
            if self._is_basic_valid(item):
                grouped[item.symbol].append(item)
        
        # 新增：分组统计日志（带频率控制）
        if self._should_log():
            logger.info(f"📊 Step5CrossCalc 检测到 {len(grouped)} 个不同合约")
        
        # 合并每个合约的OKX和币安数据
        results = []
        for symbol, items in grouped.items():
            try:
                cross_data = self._merge_pair(symbol, items)
                if cross_data:
                    results.append(cross_data)
                    self.stats["successful"] += 1
                    
            except Exception as e:
                logger.error(f"跨平台计算失败: {symbol} - {e}")
                logger.debug(f"错误详情: {traceback.format_exc()}")
                self.stats["failed"] += 1
                self.stats["calc_errors"] += 1
                continue
        
        self.stats["end_time"] = datetime.now().isoformat()
        
        # 新增：完成状态日志（带频率控制）
        if self._should_log():
            logger.info(f"✅ Step5CrossCalc 处理完成: 成功 {self.stats['successful']}/{self.stats['total_processed']}")
            logger.info(f"📈 Step5CrossCalc 统计详情: {self.stats}")
        
        return results
    
    def _is_basic_valid(self, item: Any) -> bool:
        """只做最基础的格式验证"""
        try:
            # 必须有基础属性
            if not hasattr(item, 'exchange') or not hasattr(item, 'symbol'):
                return False
            
            # 必须有价格属性（值可以任意）
            if not hasattr(item, 'latest_price'):
                return False
            
            # 必须有交易所标识
            if item.exchange not in ["okx", "binance"]:
                logger.warning(f"未知交易所: {item.exchange}")
                return False
                
            return True
        except Exception:
            return False
    
    def _merge_pair(self, symbol: str, items: List) -> Optional[CrossPlatformData]:
        """合并OKX和币安数据（只做计算，不做判断）"""
        
        # 分离OKX和币安数据
        okx_item = next((item for item in items if item.exchange == "okx"), None)
        binance_item = next((item for item in items if item.exchange == "binance"), None)
        
        # 必须两个平台都有数据
        if not okx_item or not binance_item:
            if not okx_item:
                self.stats["okx_missing"] += 1
            if not binance_item:
                self.stats["binance_missing"] += 1
            logger.debug(f"{symbol} 缺少平台数据，跳过")
            return None
        
        # 计算价格差和费率差
        try:
            # 价格计算（允许异常值）
            okx_price = self._safe_float(okx_item.latest_price)
            binance_price = self._safe_float(binance_item.latest_price)
            
            # 如果价格无效，只标记，不过滤
            if okx_price is None or binance_price is None:
                self.stats["price_invalid"] += 1
                # 继续处理，使用0值
                okx_price = okx_price or 0
                binance_price = binance_price or 0
            
            price_diff = abs(okx_price - binance_price)
            
            # 优化：更安全地计算价格百分比差
            if okx_price > 0 and binance_price > 0:
                min_price = min(okx_price, binance_price)
                if min_price > 1e-10:  # 防止除以极度接近0的数
                    price_diff_percent = (price_diff / min_price) * 100
                else:
                    price_diff_percent = 0.0
                    self.stats["price_too_low"] += 1
                    logger.debug(f"{symbol} 价格过低 ({min_price:.10f})，百分比差置0")
            else:
                price_diff_percent = 0.0
            
            # 费率计算（允许异常值）
            okx_rate = self._safe_float(okx_item.funding_rate)
            binance_rate = self._safe_float(binance_item.funding_rate)
            
            # 如果费率无效，使用0值
            okx_rate = okx_rate or 0
            binance_rate = binance_rate or 0
            rate_diff = abs(okx_rate - binance_rate)
            
        except Exception as e:
            logger.error(f"{symbol} 计算失败: {e}")
            # 记录原始数据以便调试
            logger.debug(f"原始数据 - OKX价格: {okx_item.latest_price}, 币安价格: {binance_item.latest_price}")
            self.stats["calc_errors"] += 1
            return None
        
        # 构建最终数据（保留所有原始值）
        return CrossPlatformData(
            symbol=symbol,
            price_diff=price_diff,
            price_diff_percent=price_diff_percent,
            rate_diff=rate_diff,
            
            # 必须先放没有默认值的字段
            okx_price=str(okx_item.latest_price),
            okx_funding_rate=str(okx_item.funding_rate),
            binance_price=str(binance_item.latest_price),
            binance_funding_rate=str(binance_item.funding_rate),
            
            # 再放有默认值的字段
            okx_period_seconds=okx_item.period_seconds,
            okx_countdown_seconds=okx_item.countdown_seconds,
            okx_last_settlement=okx_item.last_settlement_time,
            okx_current_settlement=okx_item.current_settlement_time,
            okx_next_settlement=okx_item.next_settlement_time,
            
            binance_period_seconds=binance_item.period_seconds,
            binance_countdown_seconds=binance_item.countdown_seconds,
            binance_last_settlement=binance_item.last_settlement_time,
            binance_current_settlement=binance_item.current_settlement_time,
            binance_next_settlement=binance_item.next_settlement_time,
        )
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """安全转换为float，不抛出异常"""
        if value is None:
            return None
        
        try:
            # 尝试直接转换
            result = float(value)
            
            # 检查特殊值
            if str(value).lower() in ['inf', '-inf', 'nan']:
                return None
                
            # 检查异常数值
            if abs(result) > 1e15:  # 防止天文数字
                return None
                
            return result
        except (ValueError, TypeError):
            try:
                # 尝试清理字符串
                cleaned = str(value).strip().replace(',', '')
                return float(cleaned)
            except:
                return None
    
    def get_detailed_report(self) -> Dict[str, Any]:
        """获取详细处理报告"""
        if self.stats["start_time"] and self.stats["end_time"]:
            start = datetime.fromisoformat(self.stats["start_time"])
            end = datetime.fromisoformat(self.stats["end_time"])
            duration = (end - start).total_seconds()
        else:
            duration = 0
        
        return {
            "statistics": self.stats,
            "processing_time_seconds": duration,
            "success_rate": self.stats["successful"] / max(1, self.stats["total_processed"]),
            "timestamp": datetime.now().isoformat()
        }
