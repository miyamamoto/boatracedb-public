#!/usr/bin/env python3
"""
日本語固定幅テキスト形式のボートレースデータパーサー
"""

import re
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class JapaneseBoatRaceTextParser:
    """日本語ボートレーステキストパーサー"""
    
    # レース場コードマッピング
    VENUE_CODES = {
        '桐生': '01', '戸田': '02', '江戸川': '03', '平和島': '04', '多摩川': '05', '浜名湖': '06',
        '蒲郡': '07', '常滑': '08', '津': '09', '三国': '10', 'びわこ': '11', '住之江': '12',
        '尼崎': '13', '鳴門': '14', '丸亀': '15', '児島': '16', '宮島': '17', '徳山': '18',
        '下関': '19', '若松': '20', '芦屋': '21', '福岡': '22', '唐津': '23', '大村': '24'
    }
    
    # 距離情報パターン（天候による動的変更対応）
    # 会場固定設定は廃止：天候により1800m→1200mに変更されるため
    
    def parse_performance_file(self, lines: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """成績ファイル（Kファイル）をパース（マルチ会場対応）"""
        result = {
            'venues': [],
            'races': [],
            'racers': [],
            'race_entries': [],
            'payouts': []
        }
        
        if not lines:
            return result
        
        try:
            # ファイルを会場単位で分割
            venue_sections = self._split_by_venue_sections(lines)
            
            for venue_name, venue_lines in venue_sections.items():
                venue_code = self._get_venue_code(venue_name)
                
                if venue_code:
                    # 会場データを追加（重複チェック）
                    if not any(v['code'] == venue_code for v in result['venues']):
                        result['venues'].append({
                            'code': venue_code,
                            'name': venue_name
                        })
                    
                    # 会場の日付を抽出
                    race_date = self._extract_date_from_venue_lines(venue_lines)
                    
                    # レース単位でデータを分割・処理
                    race_sections = self._split_into_race_sections(venue_lines)
                    
                    for race_number, race_lines in race_sections.items():
                        race_data = self._parse_race_section(
                            race_lines, venue_code, race_date, race_number
                        )
                        
                        if race_data:
                            result['races'].append(race_data['race'])
                            result['racers'].extend(race_data['racers'])
                            result['race_entries'].extend(race_data['entries'])
                            
                            # 払い戻しデータに会場・日付・レース番号を追加
                            if 'payouts' in race_data:
                                for payout in race_data['payouts']:
                                    payout['venue_code'] = venue_code
                                    payout['race_date'] = race_date
                                    payout['race_number'] = race_number
                                result['payouts'].extend(race_data['payouts'])
            
            logger.info(f"成績ファイル解析完了: {len(result['races'])}レース, "
                       f"{len(result['racers'])}選手, {len(result['race_entries'])}出走")
                       
        except Exception as e:
            logger.error(f"成績ファイル解析エラー: {e}")
        
        return result
    
    def parse_schedule_file(self, lines: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """番組表ファイル（Bファイル）をパース"""
        result = {
            'venues': [],
            'races': [],
            'racers': [],
            'race_entries': []
        }
        
        if not lines:
            return result
        
        try:
            # ヘッダー情報から基本情報を抽出
            venue_name, race_date = self._extract_header_info(lines)
            venue_code = self._get_venue_code(venue_name)
            
            if venue_code:
                result['venues'].append({
                    'code': venue_code,
                    'name': venue_name
                })
            
            # レース単位でデータを分割・処理
            race_sections = self._split_into_schedule_sections(lines)
            
            for race_number, race_lines in race_sections.items():
                race_data = self._parse_schedule_section(
                    race_lines, venue_code, race_date, race_number
                )
                
                if race_data:
                    result['races'].append(race_data['race'])
                    result['racers'].extend(race_data['racers'])
                    result['race_entries'].extend(race_data['entries'])
            
            logger.info(f"番組表ファイル解析完了: {len(result['races'])}レース, "
                       f"{len(result['racers'])}選手, {len(result['race_entries'])}出走")
                       
        except Exception as e:
            logger.error(f"番組表ファイル解析エラー: {e}")
        
        return result
    
    def _split_by_venue_sections(self, lines: List[str]) -> Dict[str, List[str]]:
        """ファイル内容を会場単位で分割（スペース・全角文字対応）"""
        venue_sections = {}
        current_venue = None
        current_lines = []
        
        for line in lines:
            line = line.strip()
            
            # 会場名パターンを検索（［成績］または［番組］を含む行）
            venue_found = False
            for venue_name in self.VENUE_CODES.keys():
                # スペースや全角文字を考慮した検索
                line_normalized = re.sub(r'[\s　]+', '', line)  # スペースと全角スペースを除去
                venue_normalized = re.sub(r'[\s　]+', '', venue_name)
                
                if (venue_normalized in line_normalized and 
                    ('［成績］' in line or '［番組］' in line)):
                    
                    # 前の会場のデータを保存
                    if current_venue and current_lines:
                        venue_sections[current_venue] = current_lines.copy()
                    
                    # 新しい会場の開始
                    current_venue = venue_name
                    current_lines = [line]
                    venue_found = True
                    logger.debug(f"会場検出: {venue_name} (行: {line})")
                    break
            
            if not venue_found and current_venue:
                current_lines.append(line)
        
        # 最後の会場のデータを保存
        if current_venue and current_lines:
            venue_sections[current_venue] = current_lines
        
        return venue_sections
    
    def _extract_date_from_venue_lines(self, lines: List[str]) -> Optional[date]:
        """会場の行から日付を抽出"""
        for line in lines:
            # 2025/ 6/ 1 形式の日付を検索
            date_match = re.search(r'(\d{4})/\s*(\d{1,2})/\s*(\d{1,2})', line)
            if date_match:
                year = int(date_match.group(1))
                month = int(date_match.group(2))
                day = int(date_match.group(3))
                try:
                    return date(year, month, day)
                except ValueError:
                    continue
        
        return None
    
    def _extract_header_info(self, lines: List[str]) -> tuple:
        """ヘッダーから会場名と開催日を抽出"""
        venue_name = None
        race_date = None
        
        for line in lines[:20]:  # 最初の20行から検索
            # 会場名の抽出（複数パターン対応）
            
            # パターン1: "ボートレース会場名" (スペースを含む場合もあり)
            venue_match1 = re.search(r'ボートレース([^　\s]*[　\s]*[^　\s]+)', line)
            if venue_match1:
                candidate = venue_match1.group(1).replace('　', '').replace(' ', '')  # スペースを除去
                if candidate in self.VENUE_CODES:
                    venue_name = candidate
            
            # パターン2: "会場名［成績］" や "会場名［番組］"
            venue_match2 = re.search(r'([^［\s]+)(?:［[^］]+］)', line)
            if venue_match2:
                candidate = venue_match2.group(1)
                if candidate in self.VENUE_CODES:
                    venue_name = candidate
            
            # パターン3: 行内の会場名検索（スペース区切り）
            if not venue_name:
                for venue in self.VENUE_CODES.keys():
                    if venue in line:
                        venue_name = venue
                        break
            
            # 日付の抽出（複数パターン対応）
            
            # パターン1: "２０１５年　１月　１日" (全角)
            date_match1 = re.search(r'([２０１２３４５６７８９０]{4})年\s*([１２３４５６７８９０]+)月\s*([１２３４５６７８９０]+)日', line)
            if date_match1:
                year_str = date_match1.group(1).translate(str.maketrans('０１２３４５６７８９', '0123456789'))
                month_str = date_match1.group(2).translate(str.maketrans('０１２３４５６７８９', '0123456789'))
                day_str = date_match1.group(3).translate(str.maketrans('０１２３４５６７８９', '0123456789'))
                try:
                    race_date = date(int(year_str), int(month_str), int(day_str))
                except ValueError:
                    pass
            
            # パターン2: "2015/ 1/ 1" (半角スラッシュ区切り)
            if not race_date:
                date_match2 = re.search(r'(\d{4})/\s*(\d{1,2})/\s*(\d{1,2})', line)
                if date_match2:
                    try:
                        year, month, day = map(int, date_match2.groups())
                        race_date = date(year, month, day)
                    except ValueError:
                        pass
        
        return venue_name, race_date
    
    def _get_venue_code(self, venue_name: str) -> Optional[str]:
        """会場名からコードを取得"""
        if not venue_name:
            return None
        return self.VENUE_CODES.get(venue_name)
    
    def _split_into_race_sections(self, lines: List[str]) -> Dict[int, List[str]]:
        """成績データをレース単位に分割"""
        sections = {}
        current_race = None
        current_lines = []
        
        for line in lines:
            # レース番号の検出 (例: "   1R", "  12R")
            race_match = re.search(r'^\s*(\d{1,2})R\s+', line)
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
    
    def _split_into_schedule_sections(self, lines: List[str]) -> Dict[int, List[str]]:
        """番組表データをレース単位に分割"""
        sections = {}
        current_race = None
        current_lines = []
        
        for line in lines:
            # レース番号の検出 (例: "１Ｒ", "１２Ｒ")
            race_match = re.search(r'([０-９]{1,2})[Ｒｒ]\s', line)
            if race_match:
                # 前のレースを保存
                if current_race is not None and current_lines:
                    sections[current_race] = current_lines.copy()
                
                # 新しいレース開始（全角数字を半角に変換）
                race_num_str = race_match.group(1)
                race_num_str = race_num_str.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
                current_race = int(race_num_str)
                current_lines = [line]
            elif current_race is not None:
                current_lines.append(line)
        
        # 最後のレースを保存
        if current_race is not None and current_lines:
            sections[current_race] = current_lines
        
        return sections
    
    def _parse_race_section(self, race_lines: List[str], venue_code: str, 
                           race_date: date, race_number: int) -> Optional[Dict]:
        """個別レースセクションをパース（成績）"""
        if not race_lines:
            return None
        
        try:
            # レース情報の抽出
            race_name = ""
            weather = None
            wind_direction = None
            wind_speed = None
            wave_height = None
            
            # ヘッダー行からレース名、距離情報、気象情報を抽出
            header_line = race_lines[0] if race_lines else ""
            distance = 1800  # デフォルト距離
            
            # レース名と距離情報の同時抽出
            # パターン: "1R       ツッキーレー                 H1200m  晴　  風  北西　 6m  波　  5cm"
            race_header_match = re.search(r'R\s+([^\s]+).*?H(\d+)m', header_line)
            if race_header_match:
                race_name = race_header_match.group(1)
                distance = int(race_header_match.group(2))
            else:
                # 距離情報がない場合はレース名のみ抽出
                name_match = re.search(r'R\s+([^\s]+)', header_line)
                if name_match:
                    race_name = name_match.group(1)
            
            # 気象情報抽出（距離情報を含む行から抽出）
            for line in race_lines[:5]:
                # 距離情報を含む行から天候情報を抽出
                # パターン: "H1200m  晴　  風  北西　 6m  波　  5cm"
                weather_match = re.search(r'H\d+m\s+([晴雨曇])\s+風\s+([^\s]+)\s+(\d+)m\s+波\s+(\d+)cm', line)
                if weather_match:
                    weather = weather_match.group(1)
                    wind_direction = self._parse_wind_direction(weather_match.group(2))
                    wind_speed = float(weather_match.group(3))
                    wave_height = float(weather_match.group(4))
                    break
                
                # 距離情報なしの天候情報（従来パターン）
                if not weather:
                    weather_match_old = re.search(r'([晴雨曇])\s+風\s+([^\s]+)\s+(\d+)m\s+波\s+(\d+)cm', line)
                    if weather_match_old:
                        weather = weather_match_old.group(1)
                        wind_direction = self._parse_wind_direction(weather_match_old.group(2))
                        wind_speed = float(weather_match_old.group(3))
                        wave_height = float(weather_match_old.group(4))
                        break
            
            # レース結果行を抽出（着順データがある行）
            entry_lines = []
            for line in race_lines:
                # 着順行の判定 (先頭スペースの有無両方に対応)
                # 例: "01  4 3776 横　井　　健　太..." または "  01  4 3776 横　井　　健　太..."
                if re.match(r'^\s{0,2}\d{2}\s+\d\s+\d{4}', line):
                    entry_lines.append(line)
            
            # レースデータ
            race_data = {
                'venue_code': venue_code,
                'race_date': race_date,
                'race_number': race_number,
                'race_name': race_name,
                'weather': weather,
                'wind_direction': wind_direction,
                'wind_speed': wind_speed,
                'wave_height': wave_height,
                'water_temperature': None,  # 成績データには水温なし
                'distance': distance  # 元データから抽出した実距離（H1200m/H1800m）
            }
            
            # 選手・出走データ
            racers = []
            entries = []
            
            for line in entry_lines:
                racer_data, entry_data = self._parse_result_line(
                    line, venue_code, race_date, race_number
                )
                if racer_data:
                    racers.append(racer_data)
                if entry_data:
                    entries.append(entry_data)
            
            # 払い戻しデータの抽出
            payouts = self._parse_payouts(race_lines)
            
            return {
                'race': race_data,
                'racers': racers,
                'entries': entries,
                'payouts': payouts
            }
            
        except Exception as e:
            logger.error(f"レースセクション解析エラー: {e}")
            return None
    
    def _parse_schedule_section(self, race_lines: List[str], venue_code: str,
                               race_date: date, race_number: int) -> Optional[Dict]:
        """個別レースセクションをパース（番組表）"""
        if not race_lines:
            return None
        
        try:
            # レース情報の抽出
            race_name = ""
            
            # ヘッダー行からレース名を抽出
            header_line = race_lines[0] if race_lines else ""
            name_match = re.search(r'予選|準優勝戦|優勝戦|一般戦', header_line)
            if name_match:
                race_name = name_match.group(0)
            
            # 選手情報行を抽出（登録番号がある行）
            entry_lines = []
            for line in race_lines:
                # 番組表の選手行の判定 (例: "1 3519冨田秀幸57福岡51B1 4.50 22.90...")
                if re.match(r'^\d\s+\d{4}', line):
                    entry_lines.append(line)
            
            # レースデータ
            race_data = {
                'venue_code': venue_code,
                'race_date': race_date,
                'race_number': race_number,
                'race_name': race_name,
                'weather': None,  # 番組表には気象情報なし
                'wind_direction': None,
                'wind_speed': None,
                'wave_height': None,
                'water_temperature': None,
                'distance': 1800  # 番組表にはH1200m/H1800m情報なし（デフォルト1800m）
            }
            
            # 選手・出走データ
            racers = []
            entries = []
            
            for line in entry_lines:
                racer_data, entry_data = self._parse_schedule_line(
                    line, venue_code, race_date, race_number
                )
                if racer_data:
                    racers.append(racer_data)
                if entry_data:
                    # 出走データにvenue_codeとrace_dateを設定
                    entry_data['venue_code'] = venue_code
                    entry_data['race_date'] = race_date
                    entries.append(entry_data)
            
            return {
                'race': race_data,
                'racers': racers,
                'entries': entries
            }
            
        except Exception as e:
            logger.error(f"番組表セクション解析エラー: {e}")
            return None
    
    def _parse_result_line(self, line: str, venue_code: str, race_date: date, 
                          race_number: int) -> tuple:
        """成績行をパース（固定幅フォーマット対応）"""
        try:
            # 成績行の固定幅フォーマット解析
            # 先頭スペースなしの形式: "01  1 3501 川　上　　昇　平 50   12  6.89   1    0.08     1.49.7"
            # 先頭スペースありの形式: "  01  1 3501 川　上　　昇　平 50   12  6.89   1    0.08     1.49.7"
            
            # 先頭スペースの有無を判定
            offset = 0
            if len(line) >= 2 and line[0:2] == '  ':
                offset = 2
            
            # データ行の最小長チェック（オフセット考慮）
            if len(line) < 48 + offset:
                return None, None
            
            try:
                # 着順のパース（オフセット適用）
                result_position_str = line[offset:offset+2].strip()
                
                # 失格・欠場・除外の場合の処理（スキップせずにデータを保存）
                if result_position_str in ['F', 'K', 'L', '0', '-', '.']:
                    # 失格・欠場・除外の場合でも選手・出走データは保存
                    # result_positionにはNullを設定
                    result_position = None
                else:
                    result_position = int(result_position_str)
                    # 1-6着以外は異常データとしてスキップ
                    if not (1 <= result_position <= 6):
                        return None, None
                
                # 固定幅でデータを抽出（オフセット適用）
                boat_number = int(line[offset+4:offset+5].strip())
                racer_number = int(line[offset+6:offset+10].strip())
                racer_name = line[offset+11:offset+19].strip()  # 選手名（全角スペース含む）
                racer_name = re.sub(r'[\s　]+', '', racer_name)  # 空白除去
                
                # モーター・ボート番号
                motor_number = None
                boat_part = None
                if len(line) > offset+21:
                    motor_str = line[offset+20:offset+22].strip()
                    if motor_str.isdigit():
                        motor_number = int(motor_str)
                if len(line) > offset+26:
                    boat_str = line[offset+25:offset+27].strip()
                    if boat_str.isdigit():
                        boat_part = int(boat_str)
                
                # 展示タイム（位置正確修正: 実際は29:33）
                exhibition_time = None
                if len(line) > offset+33:
                    exhibition_str = line[offset+29:offset+33].strip()
                    try:
                        if exhibition_str and exhibition_str != '.' and exhibition_str != '-':
                            exhibition_time = float(exhibition_str)
                    except ValueError:
                        pass
                
                # 進入コース（位置修正: 実際は37:38）
                entrance_position = None
                if len(line) > offset+38:
                    entrance_str = line[offset+37:offset+38].strip()
                    if entrance_str.isdigit():
                        entrance_position = int(entrance_str)
                
                # スタートタイミング（単位: 秒）
                st_timing = None
                if len(line) > offset+44:
                    st_str = line[offset+41:offset+45].strip()
                    try:
                        # STタイミングは通常 0.01〜0.30 の範囲（単位: 秒）
                        st_value = float(st_str)
                        # 100で割る必要がある場合の処理（例: 15 → 0.15）
                        if st_value > 1.0:
                            st_timing = st_value / 100.0
                        else:
                            st_timing = st_value
                    except ValueError:
                        pass
                
                # レースタイム（位置修正: 50文字目から、offset考慮）
                result_time = None
                if len(line) >= offset+50:  # 50文字目まであればOK
                    time_str = line[offset+50:].strip()  # 50文字目以降を読み取り
                    # "1.49.7" 形式を秒に変換
                    time_match = re.match(r'(\d)\.(\d{2})\.(\d)', time_str)
                    if time_match:
                        minutes = int(time_match.group(1))
                        seconds = int(time_match.group(2))
                        tenths = int(time_match.group(3))
                        result_time = minutes * 60 + seconds + tenths * 0.1
                
            except (ValueError, IndexError) as e:
                logger.debug(f"成績データ抽出エラー: {e}")
                return None, None
            
            # 選手データ
            racer_data = {
                'racer_number': racer_number,
                'name': racer_name,
                'birth_date': None,
                'debut_date': None,
                'racer_class': None,
                'branch': None
            }
            
            # 出走データ（展示タイム追加）
            entry_data = {
                'racer_number': racer_number,
                'boat_number': boat_number,
                'motor_number': motor_number,
                'boat_part': boat_part,
                'exhibition_time': exhibition_time,  # 展示タイム追加
                'entrance_position': entrance_position,  # 進入コース追加
                'st_timing': st_timing,
                'result_position': result_position,
                'result_time': result_time,  # レースタイム追加
                'venue_code': venue_code,
                'race_date': race_date,
                'race_number': race_number
            }
            
            return racer_data, entry_data
            
        except Exception as e:
            logger.error(f"成績行解析エラー: {e} - Line: {line}")
            return None, None
    
    def _parse_schedule_line(self, line: str, venue_code: str, race_date: date,
                            race_number: int) -> tuple:
        """番組表行をパース（固定幅フォーマット対応）"""
        try:
            # 番組表行の可変幅フォーマット解析
            # 可変位置フォーマット:
            # 位置0: 艇番（1桁）
            # 位置1: 空白
            # 位置2-5: 選手番号（4桁）
            # 位置6-: 選手名（可変長）- 数字が見つかるまで
            # その後: 年齢、支部、体重、級別（位置は動的）
            
            # データ行の最小長チェック
            if len(line) < 8:
                return None, None
            
            # 可変幅でデータを抽出
            try:
                boat_number = int(line[0:1].strip())
                racer_number = int(line[2:6].strip())
                
                # 選手名の終了位置を探す（数字が見つかるまで）
                name_end = 6
                for i in range(6, min(len(line), 20)):
                    if line[i].isdigit():
                        name_end = i
                        break
                racer_name = line[6:name_end].strip()
                
                # 年齢の抽出（動的位置）
                age = None
                if len(line) > name_end + 2:
                    age_str = line[name_end:name_end+2].strip()
                    if age_str.isdigit():
                        age = int(age_str)
                
                # 支部の抽出（2文字、動的位置）
                branch = None
                if len(line) > name_end + 4:
                    branch = line[name_end+2:name_end+4].strip()
                
                # 体重の抽出（動的位置）
                weight = None
                if len(line) > name_end + 6:
                    weight_str = line[name_end+4:name_end+6].strip()
                    if weight_str.isdigit():
                        weight = float(weight_str)
                
                # 級別の抽出（動的位置）
                racer_class = None
                if len(line) > name_end + 8:
                    racer_class = line[name_end+6:name_end+8].strip()
                
            except (ValueError, IndexError) as e:
                logger.debug(f"番組表データ抽出エラー: {e}")
                return None, None
            
            # 選手データ（年齢・体重を含む）
            racer_data = {
                'racer_number': racer_number,
                'name': racer_name,
                'age': age,  # 年齢を追加
                'weight': weight,  # 体重を追加
                'birth_date': None,
                'debut_date': None,
                'racer_class': racer_class,
                'branch': branch
            }
            
            # 出走データ
            entry_data = {
                'racer_number': racer_number,
                'boat_number': boat_number,
                'motor_number': None,  # 番組表には詳細なし
                'st_timing': None,
                'result_position': None,  # 番組表には結果なし
                'venue_code': venue_code,
                'race_date': race_date,
                'race_number': race_number
            }
            
            return racer_data, entry_data
            
        except Exception as e:
            logger.error(f"番組表行解析エラー: {e} - Line: {line}")
            return None, None
    
    def _parse_payouts(self, race_lines: List[str]) -> List[Dict[str, Any]]:
        """払い戻しデータをパース"""
        payouts = []
        
        # レース結果の後の払い戻しデータを探す
        start_idx = -1
        # 最後の着順データ（通常は06）を探す
        for i in range(len(race_lines)):
            if re.match(r'^\s*0[1-6]\s+\d\s+\d{4}', race_lines[i]):
                # 次の行が着順データでないことを確認
                if i + 1 < len(race_lines) and not re.match(r'^\s*0[1-6]\s+\d\s+\d{4}', race_lines[i + 1]):
                    start_idx = i + 1
                    # さらに空行をスキップ
                    while start_idx < len(race_lines) and race_lines[start_idx].strip() == '':
                        start_idx += 1
                    break
        
        if start_idx == -1:
            return payouts
        
        # 払い戻しデータのパース
        is_wide_section = False  # 拡連複セクション中かどうか
        is_fukusho_section = False  # 複勝セクション中かどうか
        wide_count = 0  # 拡連複の組み合わせ数
        
        for i in range(start_idx, len(race_lines)):
            line_raw = race_lines[i]  # 元の行（空白を保持）
            line = line_raw.strip()   # 判定用
            
            if line == '':
                # 空行で各セクション終了
                if is_wide_section:
                    is_wide_section = False
                    wide_count = 0
                if is_fukusho_section:
                    is_fukusho_section = False
                continue
            
            # 単勝
            if line.startswith('単勝'):
                is_wide_section = False
                is_fukusho_section = False
                # パターン1: "単勝 1 160円 1番人気" のような形式
                match = re.search(r'単勝\s+(\d+)\s+([0-9,]+)円?(?:\s+(\d+)番人気)?', line)
                if not match:
                    # パターン2: "単勝 1 160" のような形式（円なし）
                    match = re.search(r'単勝\s+(\d+)\s+(\d+)', line)
                if match:
                    payout_str = match.group(2).replace(',', '')
                    payouts.append({
                        'ticket_type': 'tansho',
                        'winning_combination': match.group(1),
                        'payout': int(payout_str),
                        'popularity': int(match.group(3)) if match.lastindex >= 3 and match.group(3) else None
                    })
            
            # 複勝（複数の組み合わせに対応）
            elif line.startswith('複勝'):
                is_wide_section = False
                is_fukusho_section = True  # 複勝セクション開始
                # パターン1: "複勝 3 950円 8番人気" のような形式（1艇ずつ別行）
                match = re.search(r'複勝\s+(\d+)\s+([0-9,]+)円?(?:\s+(\d+)番人気)?', line)
                if match:
                    payout_str = match.group(2).replace(',', '')
                    payouts.append({
                        'ticket_type': 'fukusho',
                        'winning_combination': match.group(1),
                        'payout': int(payout_str),
                        'popularity': int(match.group(3)) if match.group(3) else None
                    })
                else:
                    # パターン2: "複勝 3 950 5 190 1 150" のような形式（全て1行）
                    matches = re.findall(r'(\d+)\s+(\d+)', line[2:])
                    for boat, amount in matches:
                        payouts.append({
                            'ticket_type': 'fukusho',
                            'winning_combination': boat,
                            'payout': int(amount),
                            'popularity': None
                        })
            
            # ２連単
            elif line.startswith('２連単'):
                is_wide_section = False
                is_fukusho_section = False
                # パターン1: "２連単 1-2 1,230円 5番人気" のような形式
                match = re.search(r'２連単\s+(\d+)-(\d+)\s+([0-9,]+)円?(?:\s+(\d+)番人気)?', line)
                if not match:
                    # パターン2: "２連単 1-2 1230" のような形式（円なし、カンマなし）
                    match = re.search(r'２連単\s+(\d+)-(\d+)\s+(\d+)', line)
                if match:
                    payout_str = match.group(3).replace(',', '')
                    payouts.append({
                        'ticket_type': 'rensho',
                        'winning_combination': f"{match.group(1)}-{match.group(2)}",
                        'payout': int(payout_str),
                        'popularity': int(match.group(4)) if match.lastindex >= 4 and match.group(4) else None
                    })
            
            # ２連複
            elif line.startswith('２連複'):
                is_wide_section = False
                is_fukusho_section = False
                # パターン1: "２連複 1-2 890円 3番人気" のような形式
                match = re.search(r'２連複\s+(\d+)-(\d+)\s+([0-9,]+)円?(?:\s+(\d+)番人気)?', line)
                if not match:
                    # パターン2: "２連複 1-2 890" のような形式（円なし）
                    match = re.search(r'２連複\s+(\d+)-(\d+)\s+(\d+)', line)
                if match:
                    payout_str = match.group(3).replace(',', '')
                    payouts.append({
                        'ticket_type': 'renfuku',
                        'winning_combination': f"{match.group(1)}-{match.group(2)}",
                        'payout': int(payout_str),
                        'popularity': int(match.group(4)) if match.lastindex >= 4 and match.group(4) else None
                    })
            
            # 拡連複（複数の組み合わせに対応）
            elif line.startswith('拡連複'):
                is_wide_section = True
                is_fukusho_section = False
                wide_count = 0
                
                # 拡連複は1行に複数の組み合わせが記載される場合がある
                # 例: "拡連複 1-2 450円 2番人気 1-3 680円 5番人気 2-3 290円 1番人気"
                # または: "拡連複 1-2 450 1-3 680 2-3 290"
                
                # まず、拡連複の後の部分を取得
                wide_line = line[3:].strip()  # "拡連複"を除去
                
                # パターン1: "1-2 450円 2番人気" の繰り返し
                matches = re.findall(r'(\d+)-(\d+)\s+([0-9,]+)円?(?:\s+(\d+)番人気)?', wide_line)
                if matches:
                    for match in matches:
                        wide_count += 1
                        payout_str = match[2].replace(',', '')
                        payouts.append({
                            'ticket_type': 'wide',
                            'winning_combination': f"{match[0]}-{match[1]}",
                            'payout': int(payout_str),
                            'popularity': int(match[3]) if len(match) > 3 and match[3] else None
                        })
                else:
                    # パターン2: "1-2 450" の繰り返し（円・人気なし）
                    matches = re.findall(r'(\d+)-(\d+)\s+(\d+)', wide_line)
                    for match in matches:
                        wide_count += 1
                        payouts.append({
                            'ticket_type': 'wide',
                            'winning_combination': f"{match[0]}-{match[1]}",
                            'payout': int(match[2]),
                            'popularity': None
                        })
            
            # ３連単
            elif line.startswith('３連単'):
                is_wide_section = False
                is_fukusho_section = False
                wide_count = 0
                # パターン1: "３連単 3-5-1 6,690円 13番人気" のような形式
                match = re.search(r'３連単\s+(\d+)-(\d+)-(\d+)\s+([0-9,]+)円?(?:\s+(\d+)番人気)?', line)
                if not match:
                    # パターン2: "３連単 3-5-1 6690" のような形式（円なし、カンマなし）
                    match = re.search(r'３連単\s+(\d+)-(\d+)-(\d+)\s+(\d+)', line)
                if match:
                    payout_str = match.group(4).replace(',', '')
                    payouts.append({
                        'ticket_type': 'sansho',
                        'winning_combination': f"{match.group(1)}-{match.group(2)}-{match.group(3)}",
                        'payout': int(payout_str),
                        'popularity': int(match.group(5)) if match.lastindex >= 5 and match.group(5) else None
                    })
            
            # ３連複
            elif line.startswith('３連複'):
                is_wide_section = False
                is_fukusho_section = False
                wide_count = 0
                # パターン1: "３連複 1-3-5 2,340円 8番人気" のような形式
                match = re.search(r'３連複\s+(\d+)-(\d+)-(\d+)\s+([0-9,]+)円?(?:\s+(\d+)番人気)?', line)
                if not match:
                    # パターン2: "３連複 1-3-5 2340" のような形式（円なし、カンマなし）
                    match = re.search(r'３連複\s+(\d+)-(\d+)-(\d+)\s+(\d+)', line)
                if match:
                    payout_str = match.group(4).replace(',', '')
                    payouts.append({
                        'ticket_type': 'sanfuku',
                        'winning_combination': f"{match.group(1)}-{match.group(2)}-{match.group(3)}",
                        'payout': int(payout_str),
                        'popularity': int(match.group(5)) if match.lastindex >= 5 and match.group(5) else None
                    })
            
            # 複勝の続きの行（舟券種別名がなく、艇番号で始まる）
            elif is_fukusho_section:
                # インデントされた複勝の続き
                # 例: "         2          150円    2番人気"
                match = re.match(r'^\s+(\d+)\s+([0-9,]+)円?(?:\s+(\d+)番人気)?', line_raw)
                if match:
                    payout_str = match.group(2).replace(',', '')
                    payouts.append({
                        'ticket_type': 'fukusho',
                        'winning_combination': match.group(1),
                        'payout': int(payout_str),
                        'popularity': int(match.group(3)) if match.group(3) else None
                    })
            
            # 拡連複の続きの行（舟券種別名がなく、組み合わせで始まる）
            # 拡連複が複数行にわたる場合の処理
            elif is_wide_section and wide_count < 3:
                # 空白でインデントされた行、または直接組み合わせで始まる行
                # 例: "      1-3 680円 5番人気" または "1-3 680円 5番人気"
                match = None
                
                # パターン1: インデント付きまたはなしで "1-3 680円 5番人気" 形式
                match = re.match(r'^\s*(\d+)-(\d+)\s+([0-9,]+)円?(?:\s+(\d+)番人気)?', line)
                if not match:
                    # パターン2: "1-3 680" のような形式（円・人気なし）
                    match = re.match(r'^\s*(\d+)-(\d+)\s+(\d+)$', line)
                
                if match:
                    wide_count += 1
                    payout_str = match.group(3).replace(',', '')
                    payouts.append({
                        'ticket_type': 'wide',
                        'winning_combination': f"{match.group(1)}-{match.group(2)}",
                        'payout': int(payout_str),
                        'popularity': int(match.group(4)) if match.lastindex >= 4 and match.group(4) else None
                    })
            
            # [払戻金]で始まる行があったら終了
            elif '[払戻金]' in line:
                break
        
        return payouts
    
    def _parse_wind_direction(self, wind_str: str) -> Optional[int]:
        """風向きを数値に変換"""
        wind_map = {
            '北': 1, '北北東': 2, '北東': 3, '東北東': 4,
            '東': 5, '東南東': 6, '南東': 7, '南南東': 8,
            '南': 9, '南南西': 10, '南西': 11, '西南西': 12,
            '西': 13, '西北西': 14, '北西': 15, '北北西': 16
        }
        return wind_map.get(wind_str)
    
    def validate_parsed_data(self, result: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """パースされたデータの妥当性をチェック"""
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'statistics': {}
        }
        
        # レースデータのチェック
        races = result.get('races', [])
        if races:
            race_numbers = [r.get('race_number') for r in races]
            unique_race_numbers = set(race_numbers)
            
            # レース番号の妥当性
            if len(unique_race_numbers) == 1 and 1 in unique_race_numbers:
                validation_result['warnings'].append('全てのレース番号が1になっています')
            
            # レース番号の範囲チェック（通常1-12）
            invalid_race_numbers = [n for n in race_numbers if n is None or n < 1 or n > 12]
            if invalid_race_numbers:
                validation_result['errors'].append(f'無効なレース番号: {invalid_race_numbers}')
                validation_result['valid'] = False
        
        # 選手データのチェック
        racers = result.get('racers', [])
        if racers:
            # 年齢のチェック
            ages = [r.get('age') for r in racers if r.get('age') is not None]
            if ages:
                age_counts = {}
                for age in ages:
                    age_counts[age] = age_counts.get(age, 0) + 1
                
                # 同じ年齢が多すぎる場合は警告
                max_age_count = max(age_counts.values()) if age_counts else 0
                if max_age_count > len(ages) * 0.3:  # 30%以上が同じ年齢
                    most_common_age = max(age_counts, key=age_counts.get)
                    validation_result['warnings'].append(
                        f'{most_common_age}歳の選手が{max_age_count}人/{len(ages)}人 '
                        f'({max_age_count/len(ages)*100:.1f}%)'
                    )
            
            # 体重のチェック
            weights = [r.get('weight') for r in racers if r.get('weight') is not None]
            if weights:
                weight_counts = {}
                for weight in weights:
                    weight_counts[weight] = weight_counts.get(weight, 0) + 1
                
                # 同じ体重が多すぎる場合は警告
                max_weight_count = max(weight_counts.values()) if weight_counts else 0
                if max_weight_count > len(weights) * 0.3:  # 30%以上が同じ体重
                    most_common_weight = max(weight_counts, key=weight_counts.get)
                    validation_result['warnings'].append(
                        f'{most_common_weight}kgの選手が{max_weight_count}人/{len(weights)}人 '
                        f'({max_weight_count/len(weights)*100:.1f}%)'
                    )
            
            # 選手番号の妥当性（4桁）
            invalid_racer_numbers = [
                r.get('racer_number') for r in racers 
                if r.get('racer_number') is not None and 
                (r.get('racer_number') < 1000 or r.get('racer_number') > 9999)
            ]
            if invalid_racer_numbers:
                validation_result['errors'].append(f'無効な選手番号: {invalid_racer_numbers[:10]}...')
                validation_result['valid'] = False
        
        # エントリーデータのチェック
        entries = result.get('race_entries', [])
        if entries:
            # 展示タイムのチェック
            exhibition_times = [e.get('exhibition_time') for e in entries if e.get('exhibition_time') is not None]
            if not exhibition_times and len(entries) > 0:
                validation_result['warnings'].append('展示タイムが全て欠損しています')
            elif exhibition_times:
                # 通常6.6-7.0秒程度
                abnormal_times = [t for t in exhibition_times if t < 6.0 or t > 8.0]
                if abnormal_times:
                    validation_result['warnings'].append(
                        f'異常な展示タイム: {abnormal_times[:5]}... '
                        f'({len(abnormal_times)}/{len(exhibition_times)}件)'
                    )
            
            # スタートタイミングのチェック
            st_timings = [e.get('st_timing') for e in entries if e.get('st_timing') is not None]
            if st_timings:
                # 通常-0.3〜+0.3秒程度
                abnormal_timings = [t for t in st_timings if t < -0.5 or t > 0.5]
                if abnormal_timings:
                    validation_result['warnings'].append(
                        f'異常なスタートタイミング: {abnormal_timings[:5]}... '
                        f'({len(abnormal_timings)}/{len(st_timings)}件)'
                    )
        
        # 統計情報
        validation_result['statistics'] = {
            'total_races': len(races),
            'total_racers': len(racers),
            'total_entries': len(entries),
            'unique_race_numbers': len(unique_race_numbers) if races else 0,
            'racers_with_age': len([r for r in racers if r.get('age') is not None]),
            'racers_with_weight': len([r for r in racers if r.get('weight') is not None]),
            'entries_with_exhibition': len([e for e in entries if e.get('exhibition_time') is not None]),
            'entries_with_st_timing': len([e for e in entries if e.get('st_timing') is not None])
        }
        
        return validation_result