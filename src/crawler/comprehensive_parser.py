#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
包括的ボートレースデータパーサー
オッズ、選手詳細、モーター成績、展示走行データなど全データ形式に対応
"""

import re
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Tuple
import json

logger = logging.getLogger(__name__)

class ComprehensiveBoatRaceParser:
    """包括的ボートレースデータパーサー"""
    
    def __init__(self):
        # レース場コードマッピング（1-24）
        self.venue_codes = {
            '桐生': '01', '戸田': '02', '江戸川': '03', '平和島': '04', '多摩川': '05', '浜名湖': '06',
            '蒲郡': '07', '常滑': '08', '津': '09', '三国': '10', 'びわこ': '11', '住之江': '12',
            '尼崎': '13', '鳴門': '14', '丸亀': '15', '児島': '16', '宮島': '17', '徳山': '18',
            '下関': '19', '若松': '20', '芦屋': '21', '福岡': '22', '唐津': '23', '大村': '24'
        }
        
        # 逆引きマッピング
        self.venue_names = {v: k for k, v in self.venue_codes.items()}
        
        # 舟券種類マッピング（全角・半角対応）
        self.ticket_types = {
            '単勝': 'win',
            '複勝': 'place', 
            '２連単': 'exacta',
            '２連複': 'quinella',
            '３連単': 'trifecta',
            '３連複': 'trio',
            '拡連複': 'wide',
            # 全角バリエーション
            '３連単': 'trifecta',
            '３連複': 'trio',
            '２連単': 'exacta',
            '２連複': 'quinella'
        }
        
        # 選手級別マッピング
        self.racer_classes = {
            'A1': 1, 'A2': 2, 'B1': 3, 'B2': 4
        }
        
        # 支部マッピング
        self.branches = {
            '群馬': '01', '埼玉': '02', '東京': '03', '静岡': '04', '愛知': '05',
            '三重': '06', '滋賀': '07', '京都': '08', '大阪': '09', '兵庫': '10',
            '奈良': '11', '和歌山': '12', '岡山': '13', '広島': '14', '山口': '15',
            '徳島': '16', '香川': '17', '愛媛': '18', '高知': '19', '福岡': '20',
            '佐賀': '21', '長崎': '22'
        }
        
        logger.info("包括的パーサー初期化完了")
    
    def parse_schedule_file_comprehensive(self, lines: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """番組表ファイルの包括的解析"""
        result = {
            'venues': [],
            'races': [],
            'racers': [],
            'race_entries': [],
            'schedule_info': []  # 番組表特有の情報
        }
        
        if not lines:
            return result
        
        try:
            # 番組表の会場・日付情報を抽出
            venue_info = self._extract_schedule_venue_info(lines)
            
            if venue_info:
                # 会場データ追加
                result['venues'].append({
                    'code': venue_info['code'],
                    'name': venue_info['name']
                })
                
                # レース情報とエントリー情報を抽出
                races, entries = self._parse_schedule_races_and_entries(
                    lines, venue_info['code'], venue_info.get('date')
                )
                
                result['races'].extend(races)
                result['race_entries'].extend(entries)
                
                # 選手情報を抽出
                racers = self._extract_racers_from_schedule(lines)
                result['racers'].extend(racers)
            
            logger.info(f"番組表解析完了: 会場{len(result['venues'])}, "
                       f"レース{len(result['races'])}, 出走{len(result['race_entries'])}, "
                       f"選手{len(result['racers'])}")
            
        except Exception as e:
            logger.error(f"番組表解析エラー: {e}")
        
        return result
    def parse_performance_file_comprehensive(self, lines: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """成績ファイルの包括的解析（全データ対応）"""
        result = {
            'venues': [],
            'races': [],
            'racers': [],
            'race_entries': [],
            'odds_data': [],          # オッズデータ
            'payout_data': [],        # 払戻データ
            'motor_data': [],         # モーター成績
            'boat_data': [],          # ボート成績
            'exhibition_data': [],    # 展示走行データ
            'racer_stats': [],        # 選手統計データ
            'race_results': [],       # レース結果詳細
            'ticket_results': []      # 舟券結果
        }
        
        if not lines:
            return result
        
        try:
            # 会場単位で分割
            venue_sections = self._split_by_venue_sections_comprehensive(lines)
            
            for venue_key, venue_data in venue_sections.items():
                venue_info = venue_data['info']
                venue_lines = venue_data['lines']
                venue_code = venue_info['code']
                race_date = venue_info['date']
                
                # 会場データ追加
                if not any(v['code'] == venue_code for v in result['venues']):
                    result['venues'].append({
                        'code': venue_code,
                        'name': venue_info['name']
                    })
                
                # データ種別ごとに解析
                race_sections = self._split_into_comprehensive_sections(venue_lines)
                
                for section_type, section_data in race_sections.items():
                    if section_type == 'race_results':
                        self._parse_race_results_comprehensive(
                            section_data, venue_code, race_date, result
                        )
                    elif section_type == 'odds':
                        self._parse_odds_data(
                            section_data, venue_code, race_date, result
                        )
                    elif section_type == 'payouts':
                        self._parse_payout_data(
                            section_data, venue_code, race_date, result
                        )
                    elif section_type == 'motor_stats':
                        self._parse_motor_data(
                            section_data, venue_code, race_date, result
                        )
                    elif section_type == 'exhibition':
                        self._parse_exhibition_data(
                            section_data, venue_code, race_date, result
                        )
                    elif section_type == 'racer_profiles':
                        self._parse_racer_profiles(
                            section_data, venue_code, race_date, result
                        )
            
            # オッズデータの重複除去
            if result['odds_data']:
                unique_odds = {}
                for odds in result['odds_data']:
                    # ユニークキー: 会場・日付・レース番号・舟券種別・組み合わせ
                    key = (
                        odds['venue_code'],
                        odds['race_date'],
                        odds.get('race_number'),
                        odds.get('ticket_type'),
                        odds.get('combination')
                    )
                    # 同じキーのデータがある場合は最新（最後）のデータを保持
                    unique_odds[key] = odds
                
                result['odds_data'] = list(unique_odds.values())
                logger.info(f"オッズデータ重複除去: {len(result['odds_data'])}件（元: {len(unique_odds)}件）")
            
            logger.info(f"包括的解析完了: レース{len(result['races'])}, "
                       f"オッズ{len(result['odds_data'])}, 払戻{len(result['payout_data'])}, "
                       f"モーター{len(result['motor_data'])}, 展示{len(result['exhibition_data'])}")
            
        except Exception as e:
            logger.error(f"包括的解析エラー: {e}")
        
        return result
    
    def _split_by_venue_sections_comprehensive(self, lines: List[str]) -> Dict[Tuple[str, str, str], List[str]]:
        """会場とデータ種別を識別して分割"""
        venue_sections = {}
        current_venue_info = None
        current_lines = []
        
        for i, line in enumerate(lines):
            line = line.strip()
            
            # 会場情報の検出
            venue_info = self._detect_venue_and_date(line)
            if venue_info:
                # 前の会場データを保存
                if current_venue_info and current_lines:
                    venue_key = (current_venue_info['name'], current_venue_info['code'], str(current_venue_info.get('date', '')))
                    venue_sections[venue_key] = {'info': current_venue_info, 'lines': current_lines.copy()}
                    logger.debug(f"会場セクション保存: {venue_key}, 行数: {len(current_lines)}")
                
                # 新しい会場開始
                current_venue_info = venue_info
                current_lines = [line]
                logger.info(f"会場検出({i+1}行目): {venue_info}")
            elif current_venue_info:
                current_lines.append(line)
            else:
                # 会場が検出されていない場合、一般的なパターンもチェック
                if any(venue_name in line for venue_name in self.venue_codes.keys()):
                    logger.debug(f"会場名発見({i+1}行目): {line}")
        
        # 最後の会場データを保存
        if current_venue_info and current_lines:
            venue_key = (current_venue_info['name'], current_venue_info['code'], str(current_venue_info.get('date', '')))
            venue_sections[venue_key] = {'info': current_venue_info, 'lines': current_lines}
            logger.debug(f"最終会場セクション保存: {venue_key}, 行数: {len(current_lines)}")
        
        logger.info(f"会場セクション分割完了: {len(venue_sections)}セクション")
        return venue_sections
    
    def _detect_venue_and_date(self, line: str) -> Optional[Dict[str, Any]]:
        """行から会場情報と日付を検出"""
        # パターン1: ボートレース会場名 YYYY/MM/DD
        pattern1 = r'ボートレース([^　\s]+).*?(\d{4})/(\d{1,2})/(\d{1,2})'
        match1 = re.search(pattern1, line)
        if match1:
            venue_name = match1.group(1).replace('　', '').replace(' ', '')
            if venue_name in self.venue_codes:
                try:
                    race_date = date(int(match1.group(2)), int(match1.group(3)), int(match1.group(4)))
                    return {
                        'name': venue_name,
                        'code': self.venue_codes[venue_name],
                        'date': race_date
                    }
                except ValueError:
                    pass
        
        # パターン2: 会場名［データ種別］
        pattern2 = r'([^［\s]+)［([^］]+)］'
        match2 = re.search(pattern2, line)
        if match2:
            venue_name = match2.group(1)
            if venue_name in self.venue_codes:
                return {
                    'name': venue_name,
                    'code': self.venue_codes[venue_name],
                    'date': None,  # 別途抽出
                    'data_type': match2.group(2)
                }
        
        return None
    
    def _split_into_comprehensive_sections(self, lines: List[str]) -> Dict[str, List[str]]:
        """データ種別ごとに分割"""
        sections = {
            'race_results': [],
            'odds': [],
            'payouts': [],
            'motor_stats': [],
            'exhibition': [],
            'racer_profiles': []
        }
        
        current_section = None
        
        for line in lines:
            # データ種別の判定
            section_type = self._detect_data_section_type(line)
            if section_type:
                current_section = section_type
            
            if current_section:
                sections[current_section].append(line)
        
        return sections
    
    def _detect_data_section_type(self, line: str) -> Optional[str]:
        """行からデータ種別を判定（ユーザーサンプル対応）"""
        # 払戻データの検出
        if re.search(r'\[払戻金\]|払戻金|３連単|３連複|２連単|２連複|複勝.*=.*円', line):
            return 'payouts'
        
        # オッズデータの検出
        if re.search(r'オッズ|単勝|複勝|２連|３連', line) and not re.search(r'払戻|円', line):
            return 'odds'
        
        # 天候情報の検出
        if re.search(r'天候.*風.*波|晴.*風.*波', line):
            return 'race_results'  # レース結果に含める
        
        # モーター成績の検出
        if re.search(r'モーター|機関', line):
            return 'motor_stats'
        
        # 展示走行の検出
        if re.search(r'展示|チルト|体重', line):
            return 'exhibition'
        
        # 選手プロフィールの検出
        if re.search(r'選手|級別|支部|出身', line):
            return 'racer_profiles'
        
        # レース結果の検出（着順 艇番 登録番号のパターン）
        if re.search(r'^\s*\d+\s+\d+\s+\d{4}', line) or re.search(r'\d+R|着順|ST|タイム', line):
            return 'race_results'
        
        return None
    
    def _parse_race_results_comprehensive(self, lines: List[str], venue_code: str, 
                                         race_date: date, result: Dict) -> None:
        """レース結果の包括的解析"""
        race_sections = self._split_into_race_numbers(lines)
        
        for race_number, race_lines in race_sections.items():
            race_data = self._parse_single_race_comprehensive(
                race_lines, venue_code, race_date, race_number
            )
            
            if race_data:
                result['races'].append(race_data['race'])
                result['racers'].extend(race_data['racers'])
                result['race_entries'].extend(race_data['entries'])
    
    def _parse_odds_data(self, lines: List[str], venue_code: str, 
                        race_date: date, result: Dict) -> None:
        """オッズデータの解析（複数行の拡連複対応）"""
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # 拡連複の特別処理：続きの行も読み取る
            if '拡連複' in line:
                wide_odds = []
                
                # 最初の行を解析
                odds_data = self._parse_odds_line(line, venue_code, race_date)
                if odds_data:
                    if isinstance(odds_data, list):
                        wide_odds.extend(odds_data)
                    else:
                        wide_odds.append(odds_data)
                
                # 続きの行を確認（インデントされた行）
                j = i + 1
                while j < len(lines) and j < i + 3:  # 最大2行先まで確認
                    next_line = lines[j]
                    # インデントされた行で、組み合わせパターンがある場合
                    if re.match(r'^\s+\d+-\d+', next_line):
                        # 拡連複の続き行として解析
                        match = re.search(r'(\d+-\d+)\s+([\d,]+)\s*(?:人気\s+(\d+))?', next_line)
                        if match:
                            combination = match.group(1)
                            payout_str = match.group(2).replace(',', '')
                            payout = int(payout_str)
                            
                            wide_odds.append({
                                'venue_code': venue_code,
                                'race_date': race_date,
                                'race_number': None,  # 別途推定
                                'ticket_type': 'wide',
                                'combination': combination,
                                'payout': payout,
                                'odds': payout / 100.0,
                                'timestamp': datetime.now()
                            })
                        j += 1
                    else:
                        break
                
                # 収集した拡連複データを追加
                result['odds_data'].extend(wide_odds)
                i = j  # 処理済みの行をスキップ
                
            else:
                # 通常のオッズデータ解析
                odds_data = self._parse_odds_line(line, venue_code, race_date)
                if odds_data:
                    if isinstance(odds_data, list):
                        result['odds_data'].extend(odds_data)
                    else:
                        result['odds_data'].append(odds_data)
                i += 1
    
    def _parse_odds_line(self, line: str, venue_code: str, race_date: date) -> Optional[Dict]:
        """オッズ行の解析（拡連複の複数組み合わせ対応）"""
        results = []
        try:
            # 拡連複の特別処理: 1行に複数の組み合わせが含まれる場合
            # 例: "拡連複 1-2 140 1-3 250 2-3 180"
            if '拡連複' in line:
                wide_match = re.search(r'拡連複\s+(.+)', line)
                if wide_match:
                    wide_data = wide_match.group(1).strip()
                    # 複数の組み合わせを解析
                    pattern = r'(\d+-\d+)\s+([\d.]+)'
                    matches = re.findall(pattern, wide_data)
                    
                    for match in matches:
                        combination = match[0]
                        odds_value = float(match[1])
                        
                        results.append({
                            'venue_code': venue_code,
                            'race_date': race_date,
                            'race_number': None,  # 別途推定
                            'ticket_type': 'wide',
                            'combination': combination,
                            'payout': int(odds_value * 100) if odds_value < 100 else int(odds_value),
                            'odds': odds_value if odds_value < 100 else odds_value / 100.0,
                            'timestamp': datetime.now()
                        })
                    
                    # 複数の結果がある場合はリストで返す（呼び出し元で処理）
                    # 複勝の場合は1艇分でもリスト形式で返す（複勝は1-3着対応のため）
                    if any('複勝' in str(r.get('ticket_type', '')) or r.get('ticket_type') == 'place' for r in results):
                        return results if results else None
                    else:
                        return results if len(results) > 1 else (results[0] if results else None)
            
            # オッズ行パターン1: 舟券種別 組合せ 払戻金
            # 例: "単勝     2          130"
            # 例: "２連単   2-1        540  人気     2"
            pattern1 = r'(単勝|複勝|２連単|２連複|３連単|３連複)\s+([0-9-]+)\s+(\d+)'
            match1 = re.search(pattern1, line)
            
            if match1:
                ticket_type = self.ticket_types.get(match1.group(1))
                combination = match1.group(2)
                payout = int(match1.group(3))
                
                # 複勝の場合は単一結果でもリスト形式で返す
                result_data = {
                    'venue_code': venue_code,
                    'race_date': race_date,
                    'race_number': None,  # 別途推定
                    'ticket_type': ticket_type,
                    'combination': combination,
                    'payout': payout,
                    'odds': payout / 100.0,  # 100円当たりの払戻金から計算
                    'timestamp': datetime.now()
                }
                
                # 複勝の場合はリスト形式、その他は単一形式で返す
                if ticket_type == 'place':
                    return [result_data]
                else:
                    return result_data
            
            # オッズ行パターン1.5: 複勝の複数組み合わせオッズ
            # 例: "複勝 1 120 2 140 3 160"
            if '複勝' in line and not re.search(r'^\s*複勝\s+\d+\s+\d+\s*$', line):
                fukusho_multi_pattern = r'複勝\s+((?:\d+\s+\d+\s*)+)'
                match_multi = re.search(fukusho_multi_pattern, line)
                
                if match_multi:
                    odds_data = match_multi.group(1).strip()
                    # 艇番とオッズのペアを抽出
                    boat_odds_pairs = re.findall(r'(\d+)\s+(\d+)', odds_data)
                    
                    if boat_odds_pairs:
                        results = []
                        for boat_num, odds in boat_odds_pairs:
                            payout = int(odds)
                            results.append({
                                'venue_code': venue_code,
                                'race_date': race_date,
                                'race_number': None,  # 別途推定
                                'ticket_type': 'place',
                                'combination': boat_num,
                                'payout': payout,
                                'odds': payout / 100.0,
                                'timestamp': datetime.now()
                            })
                        
                        return results if results else None
            
            # オッズ行パターン2: レース番号 舟券種別 組合せ オッズ
            # 例: "1R 単勝 1 2.3"
            pattern2 = r'(\d+)R\s+([^　\s]+)\s+([^　\s]+)\s+([\d.]+)'
            match2 = re.search(pattern2, line)
            
            if match2:
                race_number = int(match2.group(1))
                ticket_type = self.ticket_types.get(match2.group(2))
                combination = match2.group(3)
                odds = float(match2.group(4))
                
                return {
                    'venue_code': venue_code,
                    'race_date': race_date,
                    'race_number': race_number,
                    'ticket_type': ticket_type,
                    'combination': combination,
                    'odds': odds,
                    'timestamp': datetime.now()
                }
        except Exception as e:
            logger.debug(f"オッズ行解析エラー: {e} - {line}")
        
        return None
    
    def _parse_payout_data(self, lines: List[str], venue_code: str, 
                          race_date: date, result: Dict) -> None:
        """払戻データの包括的解析（ユーザー提供サンプル対応）"""
        current_race_number = None
        
        for line in lines:
            line = line.strip()
            
            # レース番号の検出
            race_match = re.search(r'(\d{1,2})R', line)
            if race_match:
                current_race_number = int(race_match.group(1))
                continue
            
            # 払戻金セクションの検出
            if '[払戻金]' in line or '払戻金' in line:
                continue
            
            # 払戻データの解析（詳細パターン対応）
            payout_data_list = self._parse_comprehensive_payout_line(line, venue_code, race_date, current_race_number)
            if payout_data_list:
                # 常にリスト形式で返される（複勝は複数要素、その他は単一要素）
                result['payout_data'].extend(payout_data_list)
    
    def _parse_comprehensive_payout_line(self, line: str, venue_code: str, race_date: date, race_number: Optional[int]) -> Optional[List[Dict]]:
        """包括的払戻行解析（ユーザーサンプル対応）"""
        try:
            # パターン1: 舟券種別 的中組合せ 払戻金円 人気順位
            # 例: "３連単 3-5-1 6,690円 13番人気"
            # 注意: 複勝はパターン2で処理するため除外
            pattern1 = r'(３連単|３連複|２連単|２連複|単勝|拡連複)\s+([0-9-=]+)\s+([0-9,]+)円\s+(\d+)番人気'
            match1 = re.search(pattern1, line)
            
            if match1:
                ticket_type_jp = match1.group(1)
                combination = match1.group(2)
                payout_str = match1.group(3).replace(',', '')
                payout = int(payout_str)
                popularity = int(match1.group(4))
                
                # 舟券種別の変換
                ticket_type = self.ticket_types.get(ticket_type_jp)
                
                return [{
                    'venue_code': venue_code,
                    'race_date': race_date,
                    'race_number': race_number,
                    'ticket_type': ticket_type,
                    'winning_combination': combination,
                    'payout': payout,
                    'popularity': popularity,
                    'odds': payout / 100.0  # 100円あたりのオッズ
                }]
            
            # パターン2: 複勝の複数組み合わせ
            # 例: "複勝 3=950円(8番人気) 5=190円(3番人気) 1=150円(1番人気)"
            if '複勝' in line and '=' in line:
                results = []
                fukusho_pattern = r'(\d+)=([0-9,]+)円\((\d+)番人気\)'
                matches = re.findall(fukusho_pattern, line)
                
                for match in matches:
                    boat_number = match[0]
                    payout_str = match[1].replace(',', '')
                    payout = int(payout_str)
                    popularity = int(match[2])
                    
                    results.append({
                        'venue_code': venue_code,
                        'race_date': race_date,
                        'race_number': race_number,
                        'ticket_type': 'place',
                        'winning_combination': boat_number,
                        'payout': payout,
                        'popularity': popularity,
                        'odds': payout / 100.0
                    })
                
                if results:
                    logger.debug(f"複勝パターン2: {len(results)}件の複勝結果を処理 - {race_date} R{race_number}")
                return results if results else None  # 全ての複勝結果を返す
            
            # パターン3: 複勝の単一形式での処理
            # 例: "複勝 3 950円 8番人気" （単一の複勝結果のみの場合）
            if '複勝' in line and '=' not in line:
                fukusho_single_pattern = r'複勝\s+(\d+)\s+([0-9,]+)円\s+(\d+)番人気'
                match_single = re.search(fukusho_single_pattern, line)
                
                if match_single:
                    boat_number = match_single.group(1)
                    payout_str = match_single.group(2).replace(',', '')
                    payout = int(payout_str)
                    popularity = int(match_single.group(3))
                    
                    logger.debug(f"複勝パターン3: 単一複勝結果を処理 - {race_date} R{race_number} 艇{boat_number}")
                    # 単一の複勝結果もリスト形式で返す（一貫性のため）
                    return [{
                        'venue_code': venue_code,
                        'race_date': race_date,
                        'race_number': race_number,
                        'ticket_type': 'place',
                        'winning_combination': boat_number,
                        'payout': payout,
                        'popularity': popularity,
                        'odds': payout / 100.0
                    }]
            
            # パターン4: 実際のLZHファイルの複勝形式
            # 例: "        複勝     1          100  3          120  "
            if '複勝' in line and '=' not in line and '円' not in line:
                results = []
                # 複勝の後の数字とペアを抽出
                fukusho_real_pattern = r'複勝\s+((?:\d+\s+\d+\s*)+)'
                match_real = re.search(fukusho_real_pattern, line)
                
                if match_real:
                    data_part = match_real.group(1).strip()
                    # 数字のペア（艇番 払戻金）を抽出
                    number_pairs = re.findall(r'(\d+)\s+(\d+)', data_part)
                    
                    for boat_number, payout_amount in number_pairs:
                        results.append({
                            'venue_code': venue_code,
                            'race_date': race_date,
                            'race_number': race_number,
                            'ticket_type': 'place',
                            'winning_combination': boat_number,
                            'payout': int(payout_amount),
                            'popularity': None,  # LZHファイルには人気情報がない場合がある
                            'odds': int(payout_amount) / 100.0
                        })
                    
                    if results:
                        logger.debug(f"複勝パターン4: {len(results)}件の複勝結果を処理 - {race_date} R{race_number}")
                        return results
                
        except Exception as e:
            logger.debug(f"包括的払戻行解析エラー: {e} - {line}")
        
        return None
    
    def _parse_motor_data(self, lines: List[str], venue_code: str, 
                         race_date: date, result: Dict) -> None:
        """モーターデータの解析"""
        for line in lines:
            motor_data = self._parse_motor_line(line, venue_code, race_date)
            if motor_data:
                result['motor_data'].append(motor_data)
    
    def _parse_motor_line(self, line: str, venue_code: str, race_date: date) -> Optional[Dict]:
        """モーター行の解析"""
        try:
            # モーター行パターン: モーター番号 勝率 連対率 出走回数
            # 例: "1 7.23 45.6 123"
            pattern = r'(\d+)\s+([\d.]+)\s+([\d.]+)\s+(\d+)'
            match = re.search(pattern, line)
            
            if match:
                motor_number = int(match.group(1))
                win_rate = float(match.group(2))
                place_rate = float(match.group(3))
                races_count = int(match.group(4))
                
                return {
                    'venue_code': venue_code,
                    'motor_number': motor_number,
                    'win_rate': win_rate,
                    'place_rate': place_rate,
                    'races_count': races_count,
                    'period_start': race_date,
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.debug(f"モーター行解析エラー: {e} - {line}")
        
        return None
    
    def _parse_exhibition_data(self, lines: List[str], venue_code: str, 
                              race_date: date, result: Dict) -> None:
        """展示走行データの解析"""
        for line in lines:
            exhibition_data = self._parse_exhibition_line(line, venue_code, race_date)
            if exhibition_data:
                result['exhibition_data'].append(exhibition_data)
    
    def _parse_exhibition_line(self, line: str, venue_code: str, race_date: date) -> Optional[Dict]:
        """展示走行行の解析"""
        try:
            # 展示行パターン: レース番号 艇番 選手番号 展示タイム チルト角 体重
            # 例: "1 1 1234 6.83 0.5 52.0"
            pattern = r'(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s+([\d.-]+)\s+([\d.]+)'
            match = re.search(pattern, line)
            
            if match:
                race_number = int(match.group(1))
                boat_number = int(match.group(2))
                racer_number = int(match.group(3))
                exhibition_time = float(match.group(4))
                tilt_angle = float(match.group(5))
                weight = float(match.group(6))
                
                return {
                    'venue_code': venue_code,
                    'race_date': race_date,
                    'race_number': race_number,
                    'boat_number': boat_number,
                    'racer_number': racer_number,
                    'exhibition_time': exhibition_time,
                    'tilt_angle': tilt_angle,
                    'weight': weight
                }
        except Exception as e:
            logger.debug(f"展示行解析エラー: {e} - {line}")
        
        return None
    
    def _parse_racer_profiles(self, lines: List[str], venue_code: str, 
                             race_date: date, result: Dict) -> None:
        """選手プロフィールデータの解析"""
        for line in lines:
            racer_profile = self._parse_racer_profile_line(line)
            if racer_profile:
                result['racer_stats'].append(racer_profile)
    
    def _parse_racer_profile_line(self, line: str) -> Optional[Dict]:
        """選手プロフィール行の解析"""
        try:
            # 選手プロフィール行パターン: 登録番号 名前 級別 支部 勝率 連対率
            # 例: "1234 田中太郎 A1 東京 7.23 45.6"
            pattern = r'(\d+)\s+([ぁ-んァ-ンー一-龯]+)\s+([AB]\d)\s+([^　\s]+)\s+([\d.]+)\s+([\d.]+)'
            match = re.search(pattern, line)
            
            if match:
                racer_number = int(match.group(1))
                name = match.group(2)
                racer_class = match.group(3)
                branch = match.group(4)
                win_rate = float(match.group(5))
                place_rate = float(match.group(6))
                
                return {
                    'racer_number': racer_number,
                    'name': name,
                    'racer_class': racer_class,
                    'branch': branch,
                    'current_win_rate': win_rate,
                    'current_place_rate': place_rate,
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.debug(f"選手プロフィール行解析エラー: {e} - {line}")
        
        return None
    
    def _split_into_race_numbers(self, lines: List[str]) -> Dict[int, List[str]]:
        """レース番号ごとに分割"""
        sections = {}
        current_race = None
        current_lines = []
        
        for line in lines:
            # レース番号の検出
            race_match = re.search(r'(\d{1,2})R\s', line)
            if race_match:
                # 前のレースを保存
                if current_race is not None and current_lines:
                    sections[current_race] = current_lines.copy()
                
                # 新しいレース開始
                current_race = int(race_match.group(1))
                current_lines = [line]
            elif current_race is not None:
                current_lines.append(line)
        
        # 最後のレースを保存
        if current_race is not None and current_lines:
            sections[current_race] = current_lines
        
        return sections
    
    def _parse_single_race_comprehensive(self, race_lines: List[str], venue_code: str, 
                                        race_date: date, race_number: int) -> Optional[Dict]:
        """単一レースの包括的解析"""
        if not race_lines:
            return None
        
        try:
            # レース基本情報の抽出
            race_info = self._extract_race_info_comprehensive(race_lines[0])
            
            # 気象情報の抽出
            weather_info = self._extract_weather_info(race_lines)
            
            # 選手・結果データの抽出
            entry_lines = [line for line in race_lines if re.match(r'\s*\d{1,2}\s+\d\s+\d{4}', line)]
            
            racers = []
            entries = []
            
            for line in entry_lines:
                racer_data, entry_data = self._parse_result_line_comprehensive(
                    line, venue_code, race_date, race_number
                )
                if racer_data:
                    racers.append(racer_data)
                if entry_data:
                    entries.append(entry_data)
            
            # レースデータの構築
            race_data = {
                'venue_code': venue_code,
                'race_date': race_date,
                'race_number': race_number,
                'race_name': race_info.get('name', f'第{race_number}競走'),
                'grade': race_info.get('grade'),
                'distance': race_info.get('distance', 1800),
                **weather_info
            }
            
            return {
                'race': race_data,
                'racers': racers,
                'entries': entries
            }
            
        except Exception as e:
            logger.error(f"レース包括解析エラー: {e}")
            return None
    
    def _extract_race_info_comprehensive(self, header_line: str) -> Dict[str, Any]:
        """レース情報の詳細抽出"""
        info = {}
        
        # レース名の抽出
        name_match = re.search(r'R\s+([^\s]+)', header_line)
        if name_match:
            info['name'] = name_match.group(1)
        
        # グレードの抽出
        grade_match = re.search(r'(G[123]|SG|一般)', header_line)
        if grade_match:
            info['grade'] = grade_match.group(1)
        
        # 距離の抽出
        distance_match = re.search(r'(\d{4})m', header_line)
        if distance_match:
            info['distance'] = int(distance_match.group(1))
        
        return info
    
    def _extract_weather_info(self, lines: List[str]) -> Dict[str, Any]:
        """気象情報の詳細抽出"""
        weather_info = {
            'weather': None,
            'wind_direction': None,
            'wind_speed': None,
            'wave_height': None,
            'water_temperature': None
        }
        
        for line in lines[:20]:  # 最初の20行から検索（範囲拡大）
            line = line.strip()
            
            # パターン1: "天候 晴、風 北西 2m、波 2cm"（ユーザーサンプル形式）
            weather_pattern1 = r'天候\s*([晴曇雨雪]+).*?風\s*([東西南北]+)\s*(\d+)m.*?波\s*(\d+)cm'
            match1 = re.search(weather_pattern1, line)
            if match1:
                weather_info['weather'] = match1.group(1)
                weather_info['wind_direction'] = self._parse_wind_direction(match1.group(2))
                weather_info['wind_speed'] = float(match1.group(3))
                weather_info['wave_height'] = float(match1.group(4))
                logger.debug(f"天候情報抽出成功 (パターン1): {weather_info}")
                break
            
            # パターン2: "晴　風 北西 2m 波 2cm 水温 23℃"（既存形式）
            weather_match = re.search(
                r'([晴雨曇])\s+風\s+([^\s]+)\s+(\d+)m\s+波\s+(\d+)cm(?:\s+水温\s+(\d+)℃)?', 
                line
            )
            if weather_match:
                weather_info['weather'] = weather_match.group(1)
                weather_info['wind_direction'] = self._parse_wind_direction(weather_match.group(2))
                weather_info['wind_speed'] = float(weather_match.group(3))
                weather_info['wave_height'] = float(weather_match.group(4))
                if weather_match.group(5):
                    weather_info['water_temperature'] = float(weather_match.group(5))
                logger.debug(f"天候情報抽出成功 (パターン2): {weather_info}")
                break
            
            # パターン3: "晴　風 南　1m 波　1cm"（2007年以降対応）
            weather_match2 = re.search(
                r'([晴雨曇])\s+風\s+([^\s　]+)\s+(\d+)m\s+波\s+(\d+)cm', 
                line
            )
            if weather_match2:
                weather_info['weather'] = weather_match2.group(1)
                weather_info['wind_direction'] = self._parse_wind_direction(weather_match2.group(2))
                weather_info['wind_speed'] = float(weather_match2.group(3))
                weather_info['wave_height'] = float(weather_match2.group(4))
                logger.debug(f"天候情報抽出成功 (パターン3): {weather_info}")
                break
        
        return weather_info
    
    def _parse_wind_direction(self, wind_str: str) -> Optional[int]:
        """風向きを数値に変換"""
        wind_map = {
            '北': 1, '北北東': 2, '北東': 3, '東北東': 4,
            '東': 5, '東南東': 6, '南東': 7, '南南東': 8,
            '南': 9, '南南西': 10, '南西': 11, '西南西': 12,
            '西': 13, '西北西': 14, '北西': 15, '北北西': 16
        }
        return wind_map.get(wind_str)
    
    def _convert_race_time_to_seconds(self, time_str: str) -> Optional[float]:
        """レースタイムを秒に変換 (1.51.4 -> 111.4秒)"""
        try:
            if '.' in time_str:
                parts = time_str.split('.')
                if len(parts) == 3:  # 分.秒.1/10秒
                    minutes = int(parts[0])
                    seconds = int(parts[1])
                    tenth = int(parts[2])
                    return float(minutes * 60 + seconds + tenth / 10.0)
                elif len(parts) == 2:  # 秒.1/10秒
                    seconds = int(parts[0])
                    tenth = int(parts[1])
                    return float(seconds + tenth / 10.0)
            return float(time_str)
        except (ValueError, IndexError):
            return None
    
    def _parse_result_line_comprehensive(self, line: str, venue_code: str, 
                                        race_date: date, race_number: int) -> Tuple[Optional[Dict], Optional[Dict]]:
        """結果行の包括的解析（ユーザーサンプル対応）"""
        try:
            # スペース区切りで分割
            parts = line.strip().split()
            if len(parts) < 10:
                return None, None
            
            # 基本的な要素の抽出
            result_position = int(parts[0])  # 着順
            boat_number = int(parts[1])      # 艇番
            
            # 登録番号の抽出（"3519冨田秀幸"のような連結形式対応）
            racer_number_str = parts[2]
            racer_number_match = re.search(r'^(\d{4})', racer_number_str)
            if racer_number_match:
                racer_number = int(racer_number_match.group(1))
            else:
                racer_number = int(parts[2])  # 通常の数値形式
            
            # 選手名の抽出（連結形式対応）
            if re.search(r'^\d{4}[ぁ-んァ-ンー一-龯]+', parts[2]):
                # "3519冨田秀幸" のような連結形式から名前を抽出
                name_match = re.search(r'^\d{4}([ぁ-んァ-ンー一-龯]+)', parts[2])
                racer_name = name_match.group(1) if name_match else ""
            else:
                # 通常の分離形式
                racer_name = self._extract_racer_name_from_parts(parts[3:])
            
            if not racer_name:
                return None, None
            
            # 登録番号+名前の連結形式の場合の数値データ特定
            if re.search(r'^\d{4}[ぁ-んァ-ンー一-龯]+', parts[2]):
                # "3519冨田秀幸" のような形式の場合、parts[3:]から数値データ
                numeric_parts = [p for p in parts[3:] if self._is_numeric(p)]
                name_end_index = 3
            else:
                # 通常の名前分離形式の場合
                name_end_index = 3
                for i, part in enumerate(parts[3:], start=3):
                    if not self._is_numeric(part):
                        name_end_index = i + 1
                    else:
                        break
                numeric_parts = [p for p in parts[name_end_index:] if self._is_numeric(p)]
            
            # 結果行は最低6個の数値データが必要
            if len(numeric_parts) < 6:
                logger.debug(f"数値データ不足: {len(numeric_parts)}/6 (名前終了:{name_end_index}) - {line}")
                return None, None
            
            # 数値データの解析（実際のサンプル形式に合わせて修正）
            # 例: "01  4 3776 横　井　　健　太 73   39  6.83   4    0.08     1.49.7"
            #     着順 艇番 登録番号 名前 モーター ボート 展示タイム 進入 ST レースタイム
            
            if len(numeric_parts) >= 6:
                motor_number = int(numeric_parts[0])       # モーター番号
                boat_part = int(numeric_parts[1])          # ボート番号
                exhibition_time = float(numeric_parts[2])  # 展示タイム
                entrance_position = int(numeric_parts[3])  # 進入
                st_timing = float(numeric_parts[4])        # STタイミング
                race_time_str = numeric_parts[5]           # レースタイム
                weight = None                              # 結果行にはなし（番組表から取得）
                tilt_angle = None                          # 結果行にはなし
            else:
                logger.debug(f"数値データ不足（結果行）: {len(numeric_parts)}/6 - {line}")
                return None, None
            
            # レースタイムを秒に変換 (1.51.4 -> 111.4秒)
            race_time = self._convert_race_time_to_seconds(race_time_str)
            
            logger.debug(f"結果行解析成功: 着順{result_position} 艇{boat_number} {racer_name} M{motor_number} B{boat_part}")
            
            # 選手データ（詳細版）
            racer_data = {
                'racer_number': racer_number,
                'name': racer_name,
                'age': None,         # 番組表から取得
                'birth_date': None,  # 別途取得
                'debut_date': None,  # 別途取得
                'height': None,      # 別途取得
                'weight': weight,    # 結果行にはなし
                'branch': None,      # 番組表から取得
                'racer_class': None  # 別途取得
            }
            
            # 出走データ（詳細版）
            entry_data = {
                'venue_code': venue_code,
                'race_date': race_date,
                'race_number': race_number,
                'boat_number': boat_number,
                'racer_number': racer_number,
                'motor_number': motor_number,
                'boat_part': boat_part,
                'st_timing': st_timing,
                'exhibition_time': exhibition_time,
                'tilt_angle': tilt_angle,
                'weight': weight,
                'result_position': result_position,
                'result_time': race_time
            }
            
            return racer_data, entry_data
            
        except Exception as e:
            logger.error(f"結果行包括解析エラー: {e} - {line}")
            return None, None
    
    def _extract_racer_name_from_line(self, line: str, racer_number_str: str) -> str:
        """行から選手名を正確に抽出"""
        try:
            # 登録番号の位置を特定
            name_start = line.find(racer_number_str) + len(racer_number_str)
            remaining_line = line[name_start:].strip()
            
            # 日本語名前の抽出（漢字、ひらがな、カタカナ、全角スペース）
            name_match = re.search(r'^([ぁ-んァ-ンー一-龯　\s]+)', remaining_line)
            if name_match:
                racer_name = name_match.group(1).strip()
                # 余分な空白を除去
                racer_name = re.sub(r'\s+', '', racer_name)
                return racer_name
        except Exception as e:
            logger.debug(f"選手名抽出エラー: {e}")
        
        return ""
    
    def _is_numeric(self, value: str) -> bool:
        """文字列が数値（int/float）に変換可能かチェック（レースタイム形式含む）"""
        try:
            float(value)
            return True
        except ValueError:
            # レースタイム形式（1.51.4）のチェック
            if re.match(r'^\d+\.\d+\.\d+$', value):
                return True
            return False
    
    def _extract_racer_name_from_parts(self, parts: List[str]) -> str:
        """パーツリストから選手名を抽出（登録番号+名前の連結形式対応）"""
        if not parts:
            return ""
        
        # 最初のパーツが「登録番号+名前」の形式かチェック
        first_part = parts[0]
        
        # パターン1: "3519冨田秀幸" のような登録番号+名前の連結
        name_match = re.search(r'^\d{4}([ぁ-んァ-ンー一-龯]+)', first_part)
        if name_match:
            return name_match.group(1)
        
        # パターン2: 通常の名前のみ
        if not self._is_numeric(first_part):
            name_match = re.search(r'([ぁ-んァ-ンー一-龯]+)', first_part)
            if name_match:
                return name_match.group(1)
        
        # パターン3: 複数パーツに分かれた名前
        name_parts = []
        for part in parts:
            if not self._is_numeric(part):
                name_match = re.search(r'([ぁ-んァ-ンー一-龯]+)', part)
                if name_match:
                    name_parts.append(name_match.group(1))
            else:
                break
        
        return ''.join(name_parts)
    
    def _is_float(self, value: str) -> bool:
        """文字列がfloatに変換可能かチェック"""
        try:
            float(value)
            return True
        except ValueError:
            return False
    
    def validate_comprehensive_data(self, parsed_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """包括的データの検証"""
        validation_result = {
            'is_valid': True,
            'warnings': [],
            'errors': [],
            'statistics': {}
        }
        
        try:
            # 各データカテゴリの件数チェック
            categories = ['venues', 'races', 'racers', 'race_entries', 'odds_data', 'payout_data']
            for category in categories:
                count = len(parsed_data.get(category, []))
                validation_result['statistics'][category] = count
                
                if count == 0 and category in ['races', 'race_entries']:
                    validation_result['warnings'].append(f"{category}のデータが空です")
            
            # データ整合性チェック
            self._validate_data_consistency(parsed_data, validation_result)
            
            # オッズデータの妥当性チェック
            self._validate_odds_data(parsed_data.get('odds_data', []), validation_result)
            
        except Exception as e:
            validation_result['errors'].append(f"検証エラー: {e}")
            validation_result['is_valid'] = False
        
        return validation_result
    
    def _validate_data_consistency(self, parsed_data: Dict, validation_result: Dict) -> None:
        """データ整合性の検証"""
        races = parsed_data.get('races', [])
        entries = parsed_data.get('race_entries', [])
        
        # レースと出走データの整合性
        race_keys = {(r['venue_code'], r['race_date'], r['race_number']) for r in races}
        entry_keys = {(e['venue_code'], e['race_date'], e['race_number']) for e in entries}
        
        missing_races = entry_keys - race_keys
        if missing_races:
            validation_result['warnings'].append(f"出走データに対応するレースが見つかりません: {len(missing_races)}件")
    
    def _validate_odds_data(self, odds_data: List[Dict], validation_result: Dict) -> None:
        """オッズデータの妥当性検証"""
        for odds in odds_data:
            if 'odds' in odds and odds['odds'] <= 0:
                validation_result['errors'].append(f"無効なオッズ値: {odds['odds']}")
            
            if 'ticket_type' in odds and odds['ticket_type'] not in self.ticket_types.values():
                validation_result['warnings'].append(f"未知の舟券種別: {odds['ticket_type']}")
    
    def _extract_schedule_venue_info(self, lines: List[str]) -> Optional[Dict[str, Any]]:
        """番組表から会場・日付情報を抽出"""
        for line in lines[:20]:  # 最初の20行から検索
            # パターン1: ボートレース会場名　日付
            # 例: "ボートレース大　村   　６月　８日"
            pattern1 = r'ボートレース([^　\s]+[　\s]*[^　\s]*).*?(\d{1,2})月\s*(\d{1,2})日'
            match1 = re.search(pattern1, line)
            if match1:
                venue_name_raw = match1.group(1)
                # 「大　村」→「大村」のように空白・全角スペースを除去
                venue_name = venue_name_raw.replace('　', '').replace(' ', '')
                month = int(match1.group(2))
                day = int(match1.group(3))
                
                logger.debug(f"会場名検出: '{venue_name_raw}' → '{venue_name}'")
                
                if venue_name in self.venue_codes:
                    # 年の推定（現在年または前年）
                    from datetime import date
                    current_year = date.today().year
                    try:
                        race_date = date(current_year, month, day)
                        if race_date > date.today():
                            race_date = date(current_year - 1, month, day)
                    except ValueError:
                        race_date = None
                    
                    return {
                        'name': venue_name,
                        'code': self.venue_codes[venue_name],
                        'date': race_date
                    }
                else:
                    logger.debug(f"未知の会場名: '{venue_name}' (利用可能: {list(self.venue_codes.keys())})")
        
        return None
    
    def _extract_tournament_info(self, lines: List[str]) -> Dict[str, Any]:
        """大会情報の抽出"""
        tournament_info = {
            'tournament_name': None,
            'day_number': None,
            'venue_name': None,
            'date_info': None
        }
        
        for line in lines[:10]:  # 最初の10行程度から抽出
            line = line.strip()
            
            # 大会名の抽出（例: "スポーツ報知杯"）
            if 'スポーツ報知杯' in line or '杯' in line:
                tournament_match = re.search(r'([ぁ-んァ-ンー一-龯A-Za-z0-9]+杯)', line)
                if tournament_match:
                    tournament_info['tournament_name'] = tournament_match.group(1)
            
            # 第何日の抽出（例: "第　４日"）
            day_match = re.search(r'第\s*(\d+)\s*日', line)
            if day_match:
                tournament_info['day_number'] = int(day_match.group(1))
            
            # 会場名の抽出（例: "ボートレース下関"）
            venue_match = re.search(r'ボートレース([ぁ-んァ-ンー一-龯]+)', line)
            if venue_match:
                tournament_info['venue_name'] = venue_match.group(1)
            
            # 日付情報の抽出（例: "２０２５年　６月　１日"）
            date_match = re.search(r'(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日', line)
            if date_match:
                year = int(date_match.group(1))
                month = int(date_match.group(2))
                day = int(date_match.group(3))
                tournament_info['date_info'] = f"{year}-{month:02d}-{day:02d}"
        
        return tournament_info
    
    def _parse_schedule_races_and_entries(self, lines: List[str], venue_code: str, race_date) -> Tuple[List[Dict], List[Dict]]:
        """番組表からレース情報と出走情報を抽出"""
        races = []
        entries = []
        
        current_race = None
        current_race_number = None
        
        # 大会情報の抽出
        tournament_info = self._extract_tournament_info(lines)
        
        for line_num, line in enumerate(lines):
            line = line.strip()
            
            # レース番号の検出（例: "１Ｒ  予選"）
            race_match = re.search(r'(\d{1,2})[ＲR]\s+([^　\s]+)', line)
            if race_match:
                race_number = int(race_match.group(1))
                race_type = race_match.group(2)
                current_race_number = race_number
                
                # 距離の抽出（例: "Ｈ１８００ｍ"）
                distance = 1800  # デフォルト
                distance_match = re.search(r'[ＨH](\d{4})[ｍm]', line)
                if distance_match:
                    distance = int(distance_match.group(1))
                
                # 全角数字を半角に変換する関数
                def zenkaku_to_hankaku(text):
                    """全角数字を半角数字に変換"""
                    trans_table = str.maketrans('０１２３４５６７８９', '0123456789')
                    return text.translate(trans_table)
                
                # 変換後の行でパターンマッチング
                normalized_line = zenkaku_to_hankaku(line)
                
                # レース開始時刻の抽出（複数パターン対応）
                race_start_time = None
                start_patterns = [
                    r'発走時刻(\d{1,2})：(\d{2})',             # 発走時刻
                    r'発走時刻(\d{1,2}):(\d{2})',              # 半角コロン
                    r'発走予定(\d{1,2})：(\d{2})',             # 発走予定
                    r'発走予定(\d{1,2}):(\d{2})',              # 半角コロン
                    r'スタート時刻(\d{1,2})：(\d{2})',         # スタート時刻
                    r'スタート(\d{1,2})：(\d{2})',             # スタート
                    r'開始時刻(\d{1,2})：(\d{2})',             # 開始時刻
                    r'開始予定(\d{1,2})：(\d{2})'              # 開始予定
                ]
                
                for pattern in start_patterns:
                    start_time_match = re.search(pattern, normalized_line)
                    if start_time_match:
                        hour = int(start_time_match.group(1))
                        minute = int(start_time_match.group(2))
                        race_start_time = f"{hour:02d}:{minute:02d}"
                        break
                
                # 投票締切時刻の抽出（複数パターン対応）
                vote_close_time = None
                
                # 様々な表記パターンに対応（全角数字も考慮）
                vote_patterns = [
                    r'電話投票締切予定\s*(\d{1,2})：(\d{2})',  # 全角コロン（スペースあり）
                    r'電話投票締切予定(\d{1,2})：(\d{2})',     # 全角コロン
                    r'電話投票締切予定(\d{1,2}):(\d{2})',      # 半角コロン
                    r'電話投票締切(\d{1,2})：(\d{2})',         # 「予定」なし
                    r'電話投票締切(\d{1,2}):(\d{2})',          # 半角コロン＋「予定」なし
                    r'投票締切予定(\d{1,2})：(\d{2})',         # 「電話」なし
                    r'投票締切(\d{1,2})：(\d{2})',             # 簡略形
                    r'締切時刻(\d{1,2})：(\d{2})',             # 別表記
                    r'締切(\d{1,2})：(\d{2})',                 # 最短形
                    r'電話投票締切予定\s*(\d{1,2})：(\d{2})',  # スペース含む
                    r'締切予定時刻\s*(\d{1,2})：(\d{2})',      # 別表記2
                    r'発売締切(\d{1,2})：(\d{2})',             # 発売締切
                    r'発売締切予定(\d{1,2})：(\d{2})'          # 発売締切予定
                ]
                
                for pattern in vote_patterns:
                    vote_time_match = re.search(pattern, normalized_line)
                    if vote_time_match:
                        hour = int(vote_time_match.group(1))
                        minute = int(vote_time_match.group(2))
                        vote_close_time = f"{hour:02d}:{minute:02d}"
                        break
                
                # レース情報の抽出
                race_info = {
                    'venue_code': venue_code,
                    'race_date': race_date,
                    'race_number': race_number,
                    'race_start_time': race_start_time,
                    'race_name': race_type,
                    'distance': distance,
                    'vote_close_time': vote_close_time,
                    'grade': self._extract_grade_from_schedule(line),
                    'tournament_name': tournament_info.get('tournament_name'),
                    'tournament_day': tournament_info.get('day_number')
                }
                races.append(race_info)
                current_race = race_info
                
                logger.debug(f"レース検出 (行{line_num+1}): {race_number}R {race_type} {distance}m")
            
            # 出走選手の検出（艇番 選手登録番号 名前のパターン）
            elif current_race_number:
                # 改良されたパターン：艇番 4桁選手番号 名前年齢支部体重級別
                entry_match = re.search(r'^(\d)\s+(\d{4})([ぁ-んァ-ンー一-龯]+)', line)
                if entry_match:
                    boat_number = int(entry_match.group(1))
                    racer_number = int(entry_match.group(2))
                    racer_name = entry_match.group(3)
                    
                    # 追加情報の抽出
                    # パターン: 名前の後に年齢(2桁)、支部名、体重(2桁)、級別
                    # 例: 1 3519冨田秀幸57愛知51B1 4.50 22.90 4.50 19.05 63 36.67 30 34.62 321 24
                    #                             ↑全国勝率 ↑全国2連率   ↑モーター番号 ↑ボート番号
                    
                    # 基本情報の抽出
                    info_match = re.search(r'([ぁ-んァ-ンー一-龯]+?)(\d{2})([ぁ-んァ-ンー一-龯]+)(\d{2})([AB]\d)', line)
                    
                    if info_match:
                        # 名前は既に取得済み
                        age = int(info_match.group(2))  # 年齢
                        branch = info_match.group(3)     # 支部
                        weight = int(info_match.group(4))  # 体重
                        racer_class = info_match.group(5)  # 級別
                    else:
                        # フォールバック
                        age = None
                        branch = None
                        weight = None
                        racer_class = None
                        # 級別だけでも取得を試みる
                        class_match = re.search(r'([AB]\d)', line)
                        racer_class = class_match.group(1) if class_match else None
                    
                    # 詳細情報の抽出（勝率、モーター番号、ボート番号など）
                    # 例: 1 3519冨田秀幸57愛知51B1 4.50 22.90 4.50 19.05 63 36.67 30 34.62 321 24
                    motor_number = None
                    boat_equipment_number = None
                    national_win_rate = None
                    national_quinella_rate = None
                    local_win_rate = None
                    local_quinella_rate = None
                    motor_quinella_rate = None
                    boat_quinella_rate = None
                    recent_results = None
                    
                    if racer_class:
                        # 級別の後の部分を取得
                        class_pos = line.find(racer_class) + len(racer_class)
                        remaining = line[class_pos:].strip()
                        
                        # 数値をスペースで分割
                        numbers = remaining.split()
                        
                        # 番組表の標準フォーマット解析：
                        # 全国勝率 全国2連率 当地勝率 当地2連率 モーター番号 モーター2連率 ボート番号 ボート2連率 [今節成績] [早見]
                        if len(numbers) >= 8:
                            try:
                                # 勝率データの抽出
                                national_win_rate = float(numbers[0]) if self._is_float(numbers[0]) else None
                                national_quinella_rate = float(numbers[1]) if self._is_float(numbers[1]) else None
                                local_win_rate = float(numbers[2]) if self._is_float(numbers[2]) else None
                                local_quinella_rate = float(numbers[3]) if self._is_float(numbers[3]) else None
                                
                                # モーター番号（整数判定）
                                motor_candidate = numbers[4]
                                if motor_candidate.replace('.', '').isdigit() and '.' not in motor_candidate:
                                    motor_number = int(motor_candidate)
                                
                                # モーター2連率
                                motor_quinella_rate = float(numbers[5]) if self._is_float(numbers[5]) else None
                                
                                # ボート番号（整数判定）
                                boat_candidate = numbers[6]
                                if boat_candidate.replace('.', '').isdigit() and '.' not in boat_candidate:
                                    boat_equipment_number = int(boat_candidate)
                                
                                # ボート2連率
                                boat_quinella_rate = float(numbers[7]) if self._is_float(numbers[7]) else None
                                
                                # 今節成績（残りの数値）
                                if len(numbers) > 8:
                                    recent_results = ' '.join(numbers[8:])
                                
                            except (IndexError, ValueError) as e:
                                logger.debug(f"詳細情報抽出エラー: {e} - {line}")
                                pass
                    
                    if 1 <= boat_number <= 6:
                        entry_info = {
                            'venue_code': venue_code,
                            'race_date': race_date,
                            'race_number': current_race_number,
                            'boat_number': boat_number,
                            'racer_number': racer_number,
                            'racer_name': racer_name,
                            'age': age,
                            'weight': weight,
                            'branch': branch,
                            'racer_class': racer_class,
                            'motor_number': motor_number,
                            'boat_equipment_number': boat_equipment_number,
                            'national_win_rate': national_win_rate,
                            'national_quinella_rate': national_quinella_rate,
                            'local_win_rate': local_win_rate,
                            'local_quinella_rate': local_quinella_rate,
                            'motor_quinella_rate': motor_quinella_rate,
                            'boat_quinella_rate': boat_quinella_rate,
                            'recent_results': recent_results
                        }
                        entries.append(entry_info)
                        
                        logger.debug(f"出走検出 (行{line_num+1}): {current_race_number}R-{boat_number}艇 "
                                   f"{racer_number} {racer_name} ({racer_class})")
        
        return races, entries
    
    def _extract_racers_from_schedule(self, lines: List[str]) -> List[Dict]:
        """番組表から選手情報を抽出"""
        racers = []
        seen_racers = set()
        
        for line in lines:
            # 番組表の実際のデータ形式対応
            # 例: "1 3519冨田秀幸57愛知51B1 4.50 22.90 4.50 19.05 63 36.67 30 34.62 321 24"
            line = line.strip()
            
            # ヘッダーやタイトル行をスキップ（ただし選手データが含まれる行は除外）
            if any(keyword in line for keyword in ['番組表', '開催']) and not re.search(r'[3-5]\d{3}', line):
                continue
            # 選手番号は3000-5999の範囲のみ有効
            if not re.search(r'[3-5]\d{3}', line):
                continue
            
            # 番組表行の詳細解析
            detailed_racer = self._parse_schedule_racer_line_detailed(line)
            if detailed_racer:
                racer_number = detailed_racer['racer_number']
                if racer_number not in seen_racers:
                    racers.append(detailed_racer)
                    seen_racers.add(racer_number)
            else:
                # フォールバック：基本パターン
                parts = line.strip().split()
                for part in parts:
                    racer_match = re.search(r'^(\d{4})([ぁ-んァ-ンー一-龯]+)$', part)
                    if racer_match:
                        racer_number = int(racer_match.group(1))
                        racer_name = racer_match.group(2)
                        
                        if racer_number not in seen_racers:
                            racer_info = {
                                'racer_number': racer_number,
                                'name': racer_name,
                                'age': None,
                                'weight': None,
                                'branch': None,
                                'racer_class': None,
                                'national_win_rate': None,
                                'national_place_rate': None,
                                'local_win_rate': None,
                                'local_place_rate': None,
                                'motor_number': None,
                                'motor_place_rate': None,
                                'boat_number': None,
                                'boat_place_rate': None,
                                'recent_results': None
                            }
                            racers.append(racer_info)
                            seen_racers.add(racer_number)
                        break
                else:
                    # フォールバック：最小限の情報だけでも取得
                    simple_match = re.search(r'(\d{4})\s*([ぁ-んァ-ンー一-龯]+)', line)
                    if simple_match:
                        racer_number = int(simple_match.group(1))
                        racer_name = simple_match.group(2)
                        
                        if racer_number not in seen_racers:
                            racer_info = {
                                'racer_number': racer_number,
                                'name': racer_name,
                                'age': None,
                                'weight': None,
                                'branch': None,
                                'racer_class': None
                            }
                            racers.append(racer_info)
                            seen_racers.add(racer_number)
        
        return racers
    
    def _extract_race_name_from_schedule(self, line: str) -> str:
        """番組表からレース名を抽出"""
        # レース番号の後の文字列をレース名として抽出
        race_name_match = re.search(r'第\s*\d{1,2}\s*競走\s*(.+)', line)
        if race_name_match:
            return race_name_match.group(1).strip()
        return ""
    
    def _extract_grade_from_schedule(self, line: str) -> Optional[str]:
        """番組表からグレードを抽出"""
        grade_match = re.search(r'(SG|G[123]|一般)', line)
        if grade_match:
            return grade_match.group(1)
        return None
    
    def _convert_branch_code_to_name(self, branch_code: int) -> str:
        """支部コードを支部名に変換"""
        branch_mapping = {
            1: '群馬', 2: '埼玉', 3: '東京', 4: '神奈川',
            5: '静岡', 6: '愛知', 7: '三重', 8: '滋賀',
            9: '大阪', 10: '兵庫', 11: '岡山', 12: '広島',
            13: '山口', 14: '福岡', 15: '佐賀', 16: '長崎',
            17: '熊本', 18: '鹿児島'
        }
        return branch_mapping.get(branch_code, f"支部{branch_code}")
    
    def _parse_schedule_racer_line_detailed(self, line: str) -> Optional[Dict]:
        """番組表行の詳細解析（統計情報含む）"""
        try:
            # 例: "1 3519冨田秀幸57愛知51B1 4.50 22.90 4.50 19.05 63 36.67 30 34.62 321 24"
            # 艇番 登録番号+名前+年齢+支部+体重+級別 全国勝率 全国2率 当地勝率 当地2率 M番号 M2率 B番号 B2率 今節成績 早見
            
            parts = line.strip().split()
            if len(parts) < 10:  # 最低限の要素数チェック
                return None
            
            # 複合情報部分の解析 (例: "3519冨田秀幸57愛知51B1")
            if len(parts) >= 2:
                complex_part = parts[1]  # 2番目の要素
                
                # 正規表現で分解: 登録番号+名前+年齢+支部+体重+級別
                match = re.search(r'^(\d{4})([ぁ-んァ-ンー一-龯]+?)(\d{2})([ぁ-んァ-ンー一-龯]+)(\d{2})([AB]\d)$', complex_part)
                
                if match:
                    racer_number = int(match.group(1))
                    racer_name = match.group(2)
                    age = int(match.group(3))
                    branch = match.group(4)
                    weight = int(match.group(5))
                    racer_class = match.group(6)
                    
                    # 統計情報の抽出
                    result = {
                        'racer_number': racer_number,
                        'name': racer_name,
                        'age': age,
                        'weight': weight,
                        'branch': branch,
                        'racer_class': racer_class
                    }
                    
                    # 数値統計情報の抽出（可能な範囲で）
                    try:
                        if len(parts) >= 6:
                            result['national_win_rate'] = float(parts[2])     # 全国勝率
                            result['national_place_rate'] = float(parts[3])   # 全国2連対率
                            result['local_win_rate'] = float(parts[4])        # 当地勝率
                            result['local_place_rate'] = float(parts[5])      # 当地2連対率
                        
                        if len(parts) >= 10:
                            result['motor_number'] = int(parts[6])            # モーター番号
                            result['motor_place_rate'] = float(parts[7])      # モーター2連対率
                            result['boat_number'] = int(parts[8])             # ボート番号
                            result['boat_place_rate'] = float(parts[9])       # ボート2連対率
                        
                        if len(parts) >= 12:
                            result['recent_results'] = parts[10]              # 今節成績
                            result['early_info'] = parts[11] if len(parts) > 11 else None  # 早見
                    
                    except (ValueError, IndexError):
                        logger.debug(f"統計情報抽出で部分的エラー: {line}")
                    
                    return result
            
            return None
            
        except Exception as e:
            logger.debug(f"番組表詳細解析エラー: {e} - {line}")
            return None


ComprehensiveDataParser = ComprehensiveBoatRaceParser

__all__ = ["ComprehensiveBoatRaceParser", "ComprehensiveDataParser"]
