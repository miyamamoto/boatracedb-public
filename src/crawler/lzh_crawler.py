#!/usr/bin/env python3
"""
LZHベースボートレースクローラー
正しいURL (https://www1.mbrace.or.jp) からLZHファイルをダウンロードして解凍・処理
"""

import os
import io
import subprocess
import tempfile
import logging
import requests
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
import time
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from .japanese_text_parser import JapaneseBoatRaceTextParser

# 設定ファイル読み込み
try:
    from ..config import get_config
except ImportError:
    # フォールバック設定
    def get_config(key):
        defaults = {
            'data_collection.crawling.request_delay': 0.1,
            'data_collection.crawling.timeout': 60,
            'data_collection.crawling.parallel_workers': 16
        }
        return defaults.get(key, None)

logger = logging.getLogger(__name__)


class LzhCrawler:
    """LZHファイル対応ボートレースクローラー"""
    
    # データ提供開始日（2005年1月1日）
    DATA_START_DATE = date(2005, 1, 1)
    
    def __init__(self, 
                 cache_dir: str = "data/comprehensive_cache",
                 request_delay: Optional[float] = None,
                 timeout: Optional[int] = None,
                 max_workers: Optional[int] = None):
        # 設定ファイルから設定を読み込み
        self.request_delay = request_delay or get_config('data_collection.crawling.request_delay') or 0.1
        self.timeout = timeout or get_config('data_collection.crawling.timeout') or 60
        self.max_workers = max_workers or get_config('data_collection.crawling.parallel_workers') or 16
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/x-lzh-compressed, text/html, application/xhtml+xml, application/xml;q=0.9, */*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        
        # 正しいベースURL
        self.base_url = "https://www1.mbrace.or.jp"
        
        # キャッシュ設定
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # キャッシュ統計
        self.cache_stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'downloads': 0
        }
        
        # LZH解凍テスト
        self._test_lzh_support()
        
        logger.info(f"✅ LZHクローラー初期化: {self.base_url}")
        logger.info(f"📅 データ提供期間: {self.DATA_START_DATE}から現在まで")
    
    def _test_lzh_support(self):
        """LZH解凍サポートテスト"""
        try:
            # 7zip (p7zip)を試す
            result = subprocess.run(['7z'], capture_output=True, text=True)
            self.lzh_tool = '7z'
            logger.info("✅ LZH解凍ツール: 7-Zip")
            return
        except FileNotFoundError:
            pass
        
        try:
            # lhaを試す
            result = subprocess.run(['lha'], capture_output=True, text=True)
            self.lzh_tool = 'lha'
            logger.info("✅ LZH解凍ツール: lha")
            return
        except FileNotFoundError:
            pass
        
        # Python解凍を試す
        try:
            import unlzh
            self.lzh_tool = 'python'
            logger.info("✅ LZH解凍ツール: Python unlzh")
            return
        except ImportError:
            pass
        
        # 最後の手段としてPythonでの7zipインストールを試す
        logger.warning("⚠️ LZH解凍ツールが見つかりません。py7zrをインストールします...")
        try:
            subprocess.run(['pip', 'install', 'py7zr'], check=True)
            import py7zr
            self.lzh_tool = 'py7zr'
            logger.info("✅ LZH解凍ツール: py7zr")
            return
        except Exception as e:
            logger.error(f"❌ LZH解凍ツールのインストールに失敗: {e}")
            raise RuntimeError("LZH解凍ツールが利用できません")
    
    def _decompress_lzh(self, lzh_data: bytes) -> Dict[str, str]:
        """LZHファイル解凍"""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_lzh = Path(temp_dir) / "data.lzh"
                temp_lzh.write_bytes(lzh_data)
                
                if self.lzh_tool == '7z':
                    # 7zipを使用
                    result = subprocess.run([
                        '7z', 'e', str(temp_lzh), f'-o{temp_dir}', '-y'
                    ], capture_output=True, text=True)
                    
                    if result.returncode != 0:
                        logger.error(f"7z解凍エラー: {result.stderr}")
                        return {}
                
                elif self.lzh_tool == 'lha':
                    # lhaを使用
                    result = subprocess.run([
                        'lha', '-x', str(temp_lzh)
                    ], capture_output=True, text=True, cwd=temp_dir)
                    
                    if result.returncode != 0:
                        logger.error(f"lha解凍エラー: {result.stderr}")
                        return {}
                
                elif self.lzh_tool == 'py7zr':
                    # py7zrを使用
                    import py7zr
                    with py7zr.SevenZipFile(temp_lzh, mode='r') as archive:
                        archive.extractall(path=temp_dir)
                
                else:
                    logger.error(f"未サポートのLZH解凍ツール: {self.lzh_tool}")
                    return {}
                
                # 解凍されたファイルを読み込み
                extracted_files = {}
                for file_path in Path(temp_dir).iterdir():
                    if file_path.is_file() and file_path.name != "data.lzh":
                        try:
                            content = file_path.read_text(encoding='shift_jis', errors='ignore')
                            extracted_files[file_path.name] = content
                        except Exception as e:
                            logger.warning(f"ファイル読み込みエラー {file_path.name}: {e}")
                            # バイナリで読み込み、エラーを無視してデコード
                            try:
                                content = file_path.read_bytes().decode('shift_jis', errors='ignore')
                                extracted_files[file_path.name] = content
                            except:
                                pass
                
                logger.debug(f"🗜️ LZH解凍完了: {len(extracted_files)}ファイル")
                return extracted_files
                
        except Exception as e:
            logger.error(f"❌ LZH解凍エラー: {e}")
            return {}
    
    def _download_lzh_file(self, url: str, timeout: Optional[int] = None) -> Optional[bytes]:
        """LZHファイルダウンロード"""
        try:
            # リクエスト間隔制御
            if self.request_delay > 0:
                time.sleep(self.request_delay)
            
            actual_timeout = timeout or self.timeout
            logger.debug(f"🌐 LZHダウンロード: {url} (timeout={actual_timeout}s)")
            response = self.session.get(url, timeout=actual_timeout)
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '')
                if 'lzh' in content_type.lower() or 'compressed' in content_type.lower():
                    logger.debug(f"✅ LZHファイル取得: {len(response.content)}バイト")
                    return response.content
                else:
                    logger.warning(f"⚠️ 予期しないContent-Type: {content_type}")
                    return response.content  # 一応返す
            else:
                logger.warning(f"⚠️ HTTPエラー {response.status_code}: {url}")
                return None
                
        except Exception as e:
            logger.error(f"❌ ダウンロードエラー {url}: {e}")
            return None
    
    def _get_lzh_url(self, target_date: date, data_type: str) -> str:
        """LZH URLパターン生成"""
        year_month = target_date.strftime('%Y%m')
        day = target_date.strftime('%d')
        year_short = target_date.strftime('%y%m')
        
        if data_type == 'performance':
            # 成績データ: /od2/K/YYYYMM/kYYMMDD.lzh
            return f"{self.base_url}/od2/K/{year_month}/k{year_short}{day}.lzh"
        elif data_type == 'schedule':
            # 番組表データ: /od2/B/YYYYMM/bYYMMDD.lzh
            return f"{self.base_url}/od2/B/{year_month}/b{year_short}{day}.lzh"
        else:
            raise ValueError(f"未対応のデータ種別: {data_type}")
    
    def _get_cache_path(self, target_date: date, data_type: str) -> Path:
        """キャッシュファイルパス生成"""
        year = target_date.strftime('%Y')
        month = target_date.strftime('%m')
        day = target_date.strftime('%d')
        
        cache_path = self.cache_dir / year / month / f"lzh_{data_type}_{year}{month}{day}.cache"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        return cache_path
    
    def _is_cache_valid(self, target_date: date) -> bool:
        """キャッシュ有効性判定（CLAUDE.md準拠）"""
        today = date.today()
        
        # 前日・当日・翌日は常に最新データをダウンロード
        if target_date >= today - timedelta(days=1) and target_date <= today + timedelta(days=1):
            return False
        
        # それ以外の過去の確定データはキャッシュを無期限で使用
        return True
    
    def _load_from_cache(self, cache_path: Path) -> Optional[Dict[str, str]]:
        """キャッシュから読み込み"""
        try:
            if not cache_path.exists():
                return None
            
            import pickle
            with open(cache_path, 'rb') as f:
                cache_data = pickle.load(f)
            
            logger.debug(f"📖 キャッシュ読み込み: {cache_path}")
            return cache_data.get('data', {})
            
        except Exception as e:
            logger.error(f"❌ キャッシュ読み込みエラー {cache_path}: {e}")
            return None
    
    def _save_to_cache(self, cache_path: Path, data: Dict[str, str]):
        """キャッシュに保存"""
        try:
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            
            import pickle
            with open(cache_path, 'wb') as f:
                pickle.dump(cache_data, f)
            
            logger.debug(f"💾 キャッシュ保存: {cache_path}")
            
        except Exception as e:
            logger.error(f"❌ キャッシュ保存エラー {cache_path}: {e}")
    
    def _validate_date(
        self,
        target_date: date,
        data_type: str,
        today: Optional[date] = None,
    ) -> bool:
        """日付の有効性検証

        番組表は翌日分まで先行公開されることがあるため、`schedule` のみ翌日まで許可する。
        成績などの確定系データは当日までに制限する。
        """
        today = today or date.today()
        
        if target_date < self.DATA_START_DATE:
            logger.warning(f"⚠️ データ提供開始日より前の日付: {target_date} (開始日: {self.DATA_START_DATE})")
            return False
        
        max_allowed_date = today + timedelta(days=1) if data_type == "schedule" else today
        if target_date > max_allowed_date:
            logger.warning(
                f"⚠️ 未来の日付: {target_date} (今日: {today}, 種別: {data_type}, 上限: {max_allowed_date})"
            )
            return False
        
        return True
    
    def download_race_data(self, target_date: date, data_type: str) -> Dict[str, str]:
        """レースデータダウンロード"""
        try:
            # 日付検証
            if not self._validate_date(target_date, data_type):
                logger.error(f"❌ 無効な日付: {target_date}")
                return {}
            
            # キャッシュチェック
            cache_path = self._get_cache_path(target_date, data_type)
            
            if self._is_cache_valid(target_date):
                cached_data = self._load_from_cache(cache_path)
                if cached_data:
                    self.cache_stats['cache_hits'] += 1
                    logger.debug(f"📖 キャッシュヒット: {target_date} {data_type}")
                    return cached_data
            
            # LZHファイルダウンロード
            self.cache_stats['cache_misses'] += 1
            self.cache_stats['downloads'] += 1
            url = self._get_lzh_url(target_date, data_type)
            lzh_data = self._download_lzh_file(url)
            
            if not lzh_data:
                logger.warning(f"❌ LZHダウンロード失敗: {target_date} {data_type}")
                return {}
            
            # LZH解凍
            extracted_files = self._decompress_lzh(lzh_data)
            
            if not extracted_files:
                logger.warning(f"❌ LZH解凍失敗: {target_date} {data_type}")
                return {}
            
            # 成功時はキャッシュに保存
            self._save_to_cache(cache_path, extracted_files)
            
            logger.info(f"✅ {data_type}データ取得: {target_date}, ファイル数: {len(extracted_files)}")
            return extracted_files
            
        except Exception as e:
            logger.error(f"❌ データダウンロードエラー {target_date} {data_type}: {e}")
            return {}
    
    def get_cached_data(self, target_date: date, data_type: str) -> Optional[Dict[str, str]]:
        """キャッシュからデータを取得"""
        cache_path = self._get_cache_path(target_date, data_type)
        if cache_path.exists():
            return self._load_from_cache(cache_path)
        return None
    
    def get_cache_statistics(self) -> Dict[str, int]:
        """キャッシュ統計取得"""
        return self.cache_stats.copy()
    
    def reset_cache_statistics(self):
        """キャッシュ統計リセット"""
        self.cache_stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'downloads': 0
        }
    
    def download_multiple_dates(self, 
                               dates: List[date], 
                               data_type: str,
                               max_workers: Optional[int] = None) -> Dict[date, Dict[str, str]]:
        """複数日付の並列ダウンロード"""
        if not dates:
            return {}
        
        actual_max_workers = max_workers or self.max_workers
        logger.info(f"🚀 並列ダウンロード開始: {len(dates)}日分, ワーカー数: {actual_max_workers}")
        logger.info(f"⚙️ 設定: リクエスト間隔={self.request_delay}s, タイムアウト={self.timeout}s")
        results = {}
        
        with ThreadPoolExecutor(max_workers=actual_max_workers) as executor:
            # 並列タスク実行
            future_to_date = {
                executor.submit(self.download_race_data, target_date, data_type): target_date
                for target_date in dates
            }
            
            # 結果を取得
            for future in as_completed(future_to_date):
                target_date = future_to_date[future]
                try:
                    result = future.result()
                    results[target_date] = result
                    if result:
                        logger.debug(f"✅ 並列ダウンロード完了: {target_date}")
                    else:
                        logger.warning(f"⚠️ 並列ダウンロード失敗: {target_date}")
                except Exception as e:
                    logger.error(f"❌ 並列ダウンロードエラー {target_date}: {e}")
                    results[target_date] = {}
        
        success_count = sum(1 for result in results.values() if result)
        logger.info(f"🏁 並列ダウンロード完了: 成功={success_count}/{len(dates)}")
        return results


class LzhPerformanceCrawler(LzhCrawler):
    """LZH成績クローラー"""
    
    def __init__(self, cache_dir: str = "data/comprehensive_cache"):
        super().__init__(cache_dir)
        self.parser = JapaneseBoatRaceTextParser()
    
    def crawl_and_process(self, target_date: Optional[date] = None) -> Dict[str, List[Dict[str, Any]]]:
        """成績データクロール"""
        if target_date is None:
            target_date = date.today()
        
        logger.info(f"🏁 LZH成績クロール開始: {target_date}")
        
        # 成績データダウンロード
        performance_files = self.download_race_data(target_date, 'performance')
        
        if not performance_files:
            logger.warning(f"⚠️ 成績データなし: {target_date}")
            return {'venues': [], 'races': [], 'racers': [], 'race_entries': []}
        
        # データパース
        try:
            parsed_data = self.parser.parse_text_files(performance_files)
            logger.info(f"✅ 成績データパース完了: {target_date}")
            return parsed_data
        except Exception as e:
            logger.error(f"❌ パースエラー {target_date}: {e}")
            return {'venues': [], 'races': [], 'racers': [], 'race_entries': []}
    
    def crawl_multiple_dates(self, 
                           dates: List[date], 
                           max_workers: Optional[int] = None) -> Dict[date, Dict[str, List[Dict[str, Any]]]]:
        """複数日付の並列成績クロール"""
        if not dates:
            return {}
            
        actual_max_workers = max_workers or self.max_workers
        logger.info(f"🏁 並列成績クロール開始: {len(dates)}日分, ワーカー数: {actual_max_workers}")
        
        # 並列ダウンロード実行
        raw_results = self.download_multiple_dates(dates, 'performance', actual_max_workers)
        
        # パースデータ変換
        processed_results = {}
        for target_date, performance_files in raw_results.items():
            if performance_files:
                try:
                    # パース処理
                    parsed_data = self.parser.parse_text_files(performance_files)
                    processed_results[target_date] = parsed_data
                    logger.debug(f"✅ パース完了: {target_date}")
                except Exception as e:
                    logger.error(f"❌ パースエラー {target_date}: {e}")
                    processed_results[target_date] = {}
            else:
                processed_results[target_date] = {}
        
        success_count = sum(1 for result in processed_results.values() if result)
        logger.info(f"🏁 並列成績クロール完了: 成功={success_count}/{len(dates)}")
        return processed_results
    
    def _parse_performance_file(self, filename: str, content: str, target_date: date) -> Dict[str, List[Dict[str, Any]]]:
        """成績ファイルパース"""
        if len(content) < 100:
            logger.warning(f"⚠️ コンテンツが短すぎる: {filename} ({len(content)}文字)")
            return {'venues': [], 'races': [], 'racers': [], 'race_entries': []}
        
        # テキストを行に分割
        lines = content.split('\n')
        
        # 日本語パーサーでパース
        parsed_data = self.parser.parse_performance_file(lines)
        
        logger.debug(f"📋 パース結果 {filename}: "
                    f"{len(parsed_data['races'])}レース, "
                    f"{len(parsed_data['racers'])}選手")
        
        return parsed_data
    
    def _deduplicate_venues(self, venues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """会場の重複を除去"""
        unique_venues = {}
        for venue in venues:
            if venue['code'] not in unique_venues:
                unique_venues[venue['code']] = venue
        return list(unique_venues.values())
    
    def _deduplicate_racers(self, racers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """選手の重複を除去"""
        unique_racers = {}
        for racer in racers:
            if 'racer_number' in racer and racer['racer_number']:
                if racer['racer_number'] not in unique_racers:
                    unique_racers[racer['racer_number']] = racer
        return list(unique_racers.values())


class LzhScheduleCrawler(LzhCrawler):
    """LZH番組表クローラー"""
    
    def __init__(self, cache_dir: str = "data/comprehensive_cache"):
        super().__init__(cache_dir)
        self.parser = JapaneseBoatRaceTextParser()
    
    def crawl_and_process(self, target_date: Optional[date] = None) -> Dict[str, List[Dict[str, Any]]]:
        """番組表データクロール"""
        if target_date is None:
            target_date = date.today()
        
        logger.info(f"📅 LZH番組表クロール開始: {target_date}")
        
        # 番組表データダウンロード
        schedule_files = self.download_race_data(target_date, 'schedule')
        
        if not schedule_files:
            logger.warning(f"⚠️ 番組表データなし: {target_date}")
            return {'venues': [], 'races': [], 'racers': [], 'race_entries': []}
        
        # データパース
        all_parsed_data = {
            'venues': [],
            'races': [],
            'racers': [],
            'race_entries': []
        }
        
        for filename, content in schedule_files.items():
            try:
                parsed = self._parse_schedule_file(filename, content, target_date)
                if parsed:
                    # データを統合
                    all_parsed_data['venues'].extend(parsed['venues'])
                    all_parsed_data['races'].extend(parsed['races'])
                    all_parsed_data['racers'].extend(parsed['racers'])
                    all_parsed_data['race_entries'].extend(parsed['race_entries'])
            except Exception as e:
                logger.error(f"❌ ファイルパースエラー {filename}: {e}")
        
        # 重複を除去
        all_parsed_data['venues'] = self._deduplicate_venues(all_parsed_data['venues'])
        all_parsed_data['racers'] = self._deduplicate_racers(all_parsed_data['racers'])
        
        logger.info(f"✅ LZH番組表クロール完了: "
                   f"{len(all_parsed_data['races'])}レース, "
                   f"{len(all_parsed_data['racers'])}選手, "
                   f"{len(all_parsed_data['race_entries'])}出走")
        
        return all_parsed_data
    
    def _parse_schedule_file(self, filename: str, content: str, target_date: date) -> Dict[str, List[Dict[str, Any]]]:
        """番組表ファイルパース"""
        if len(content) < 100:
            logger.warning(f"⚠️ コンテンツが短すぎる: {filename} ({len(content)}文字)")
            return {'venues': [], 'races': [], 'racers': [], 'race_entries': []}
        
        # テキストを行に分割
        lines = content.split('\n')
        
        # 日本語パーサーでパース
        parsed_data = self.parser.parse_schedule_file(lines)
        
        logger.debug(f"📋 パース結果 {filename}: "
                    f"{len(parsed_data['races'])}レース, "
                    f"{len(parsed_data['racers'])}選手")
        
        return parsed_data
    
    def _deduplicate_venues(self, venues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """会場の重複を除去"""
        unique_venues = {}
        for venue in venues:
            if venue['code'] not in unique_venues:
                unique_venues[venue['code']] = venue
        return list(unique_venues.values())
    
    def _deduplicate_racers(self, racers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """選手の重複を除去"""
        unique_racers = {}
        for racer in racers:
            if 'racer_number' in racer and racer['racer_number']:
                if racer['racer_number'] not in unique_racers:
                    unique_racers[racer['racer_number']] = racer
        return list(unique_racers.values())
