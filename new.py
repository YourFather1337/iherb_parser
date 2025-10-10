import re
import time
import random
import logging
import cloudscraper
import lxml.html
import pandas as pd
import json
import xml.etree.ElementTree as ET
from xml.dom import minidom
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import os
import gc
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Глобальные счетчики
processed_counter = 0
failed_counter = 0
counter_lock = Lock()

class SessionManager:
    """Менеджер сессий для потоков"""
    def __init__(self):
        self.sessions = {}
        self.lock = Lock()
    
    def get_session(self, thread_id):
        with self.lock:
            if thread_id not in self.sessions:
                self.sessions[thread_id] = self._create_session()
            return self.sessions[thread_id]
    
    def _create_session(self):
        scraper = cloudscraper.create_scraper()
        # Оптимизированные настройки
        retry_strategy = Retry(
            total=2,  # Всего 2 попытки
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy, 
            pool_connections=20, 
            pool_maxsize=20
        )
        scraper.mount("http://", adapter)
        scraper.mount("https://", adapter)
        return scraper
    
    def close_all(self):
        for session in self.sessions.values():
            try:
                session.close()
            except:
                pass
        self.sessions.clear()

# Глобальный менеджер сессий
session_manager = SessionManager()

def get_random_user_agent():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
    ]
    return random.choice(user_agents)

def get_pages():
    starting_urls = (
        'https://www.iherb.com/sitemaps/products-0-www-0.xml',
    )
    all_links = []
    scraper = cloudscraper.create_scraper()
    
    for url in starting_urls:
        try:
            response = scraper.get(url, timeout=30)
            response.raise_for_status()
            tree = lxml.html.fromstring(response.content)
            links = tree.xpath('//loc/text()')
            all_links.extend(links)
            logger.info(f"Загружено {len(links)} ссылок из {url}")
        except Exception as e:
            logger.error(f"Ошибка при загрузке {url}: {e}")
            continue
    
    return all_links

def fetch_item_json(link, total_links, index):
    """Оптимизированная функция парсинга с таймаутами"""
    import threading
    thread_id = threading.current_thread().ident
    
    # Получаем сессию для текущего потока
    scraper_session = session_manager.get_session(thread_id)
    
    item_id = link.strip().split('/')[-1]
    if not item_id:
        logger.warning(f"[{index+1}/{total_links}] Не удалось извлечь ID из ссылки: {link}")
        return None

    product_url = f"https://catalog.app.iherb.com/product/{item_id}"
    recommendations_url = f"https://catalog.app.iherb.com/recommendations/freqpurchasedtogether?productId={item_id}&pageSize=2&page=1"
    
    product_data = None
    recommendations_data = None

    try:
        # Основной запрос с таймаутом
        try:
            response = scraper_session.get(product_url, timeout=15)
            if response.status_code == 200:
                product_data = response.json()
            else:
                logger.warning(f"[{index+1}/{total_links}] Статус {response.status_code} для {product_url}")
                return None
        except Exception as e:
            logger.warning(f"[{index+1}/{total_links}] Таймаут основного запроса: {e}")
            return None

        # Запрос рекомендаций (пробуем, но не критично если упадет)
        try:
            response = scraper_session.get(recommendations_url, timeout=10)
            if response.status_code == 200:
                recommendations_data = response.json()
        except:
            pass  # Рекомендации не критичны

        # Запрос UPC (тоже не критичен)
        if product_data:
            ugc_api_url = f"https://www.iherb.com/ugc/api/product/{item_id}"
            try:
                response = scraper_session.get(ugc_api_url, timeout=8)
                if response.status_code == 200:
                    ugc_data = response.json()
                    if ugc_data and ugc_data.get("upcCode"):
                        product_data["upcCode"] = ugc_data["upcCode"]
            except:
                pass  # UPC не критичен
            
            product_data["frequently_purchased_together"] = recommendations_data
            
            # Обновляем счетчик
            global processed_counter
            with counter_lock:
                processed_counter += 1
                if processed_counter % 500 == 0:  # Реже выводим прогресс
                    logger.info(f"=== ОБРАБОТАНО: {processed_counter}/{total_links} ===")
            
            return product_data
        else:
            return None
            
    except Exception as e:
        logger.error(f"[{index+1}/{total_links}] Ошибка: {e}")
        global failed_counter
        with counter_lock:
            failed_counter += 1
        return None
    finally:
        # Короткая случайная задержка
        time.sleep(random.uniform(0.1, 0.3))

def get_items_json_threaded_batched(links, max_workers=40, batch_size=800):
    """Оптимизированная батчевая обработка"""
    
    def process_chunk(chunk_links, chunk_indices):
        """Обрабатывает чанк ссылок"""
        chunk_results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_item_json, link, len(links), idx): link 
                for link, idx in zip(chunk_links, chunk_indices)
            }
            
            for future in as_completed(futures):
                link = futures[future]
                try:
                    result = future.result(timeout=30)  # Таймаут на future
                    if result:
                        chunk_results.append(result)
                except Exception as e:
                    logger.warning(f"Таймаут future для {link}: {e}")
                    # Помечаем ссылку как неудачную
                    global failed_counter
                    with counter_lock:
                        failed_counter += 1
        
        return chunk_results
    
    # Разбиваем на большие чанки для эффективности
    for chunk_start in range(0, len(links), batch_size):
        chunk_end = min(chunk_start + batch_size, len(links))
        chunk_links = links[chunk_start:chunk_end]
        chunk_indices = list(range(chunk_start, chunk_end))
        
        logger.info(f"Обработка чанка {chunk_start}-{chunk_end-1} "
                   f"({chunk_end-chunk_start} ссылок)")
        
        chunk_results = process_chunk(chunk_links, chunk_indices)
        
        # Легкая очистка памяти каждые 4 чанка
        if chunk_start % (batch_size * 4) == 0:
            gc.collect()
        
        if chunk_results:
            yield chunk_results
        
        # Минимальная пауза между чанками
        if chunk_end < len(links):
            time.sleep(1)

def get_data(items_json_batch):
    items_data = []
    for i, item in enumerate(items_json_batch):
        if item and item.get('id'):
            try:
                title = item.get('displayName', '')
                brand = item.get('brandName', '')
                link = item.get('url', '')
                image_indices = item.get('imageIndices', [])
                image_indices_360 = item.get('imageIndices360', [])
                
                brand_code = item.get('brandCode', '').lower() if item.get('brandCode') else ''
                category = item.get('rootCategoryName', '')
                category_id = item.get('rootCategoryId', '')
                part_num = item.get('partNumber', '').lower().replace('-', '') if item.get('partNumber') else ''
                
                regular_image_links = []
                if brand_code and part_num:
                    for idx in image_indices:
                        regular_image_links.append(f'https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/{brand_code}/{part_num}/v/{idx}.jpg')
                
                _360_image_links = []
                if brand_code and part_num:
                    for idx in image_indices_360:
                        _360_image_links.append(f'https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/{brand_code}/{part_num}/v/{idx}.jpg')

                # If no regular images and primaryImageIndex exists, use it as a regular image
                if not regular_image_links and item.get('primaryImageIndex'):
                    regular_image_links.append(f'https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/{brand_code}/{part_num}/v/{item.get("primaryImageIndex")}.jpg')

                item_id = item.get("id", "")
                package = item.get("packageQuantity", "")

                price_info = item.get("listPrice", {})
                if isinstance(price_info, dict):
                    price = price_info.get("amount", "")
                    currency = price_info.get("currencyCode", "USD")
                else:
                    price_str = str(price_info)
                    price = price_str[1:] if len(price_str) > 1 and price_str[0] in ["$", "€", "£"] else price_str
                    currency = "USD" if price_str.startswith("$") else "EUR" if price_str.startswith("€") else "GBP" if price_str.startswith("£") else "USD"

                availability = "Available" if bool(item.get("isAvailableToPurchase")) else "Unavailable"
                dimensions = item.get("dimensions", "")
                weight_info = item.get("actualWeight", {})
                if isinstance(weight_info, dict):
                    weight = f"{weight_info.get('amount', '')} {weight_info.get('unit', '')}".strip()
                else:
                    weight = str(weight_info)

                expiration_date = item.get("formattedExpirationDate", "")
                rating = item.get("averageRating", "")
                total_rating_count = item.get("totalRatingCount", "")
                recent_activity_message = item.get("recentActivityMessage", "")
                upc_code = item.get("upcCode", "")

                product_rankings = []
                if item.get("productRanks"):
                    for rank_info in item.get("productRanks"):
                        product_rankings.append(f"{rank_info.get('categoryDisplayName', '')}: {rank_info.get('rank', '')}")

                # Extracting Brand Path and Category Path
                all_brand_paths = []
                all_category_paths = []
                canonical_paths = item.get("canonicalPaths", [])
                for path in canonical_paths:
                    path_display_names = [p.get("displayName", "") for p in reversed(path)]
                    path_str = " > ".join(path_display_names)
                    
                    if path_display_names and "Brands A-Z" in path_display_names[0]:
                        all_brand_paths.append(path_str)
                    elif path_display_names and "Categories" in path_display_names[0]:
                        all_category_paths.append(path_str)

                # Combining Origin Product and Frequently Purchased Together products
                combined_related_products = []
                if item.get('frequently_purchased_together') and item['frequently_purchased_together'].get('originProduct'):
                    op = item['frequently_purchased_together']['originProduct']
                    combined_related_products.append(f"Current item: {op.get('name', '')}, {op.get('listPrice', '')}")

                if item.get('frequently_purchased_together') and item['frequently_purchased_together'].get('recommendedProducts'):
                    for rec_product in item['frequently_purchased_together']['recommendedProducts']:
                        combined_related_products.append(f"{rec_product.get('name', '')}, {rec_product.get('listPrice', '')}")

                description = item.get("description", "")
                if description:
                    description = description.replace("</li>", "\n").replace("</p>", "\n").replace("<br>", "\n").replace("<br/>", "\n").replace("&nbsp;", " ")
                    clean = re.compile("<.*?>")
                    description = re.sub(clean, "", description).strip()

                product_details_str = ""
                if expiration_date: product_details_str += f"\n• Best by: {expiration_date}"
                if item.get("formattedOnSaleDate"): product_details_str += f"\n• First available: {item.get('formattedOnSaleDate')}"
                if weight: product_details_str += f"\n• Shipping weight: {weight}"
                if part_num: product_details_str += f"\n• Product code: {part_num}"
                if upc_code: product_details_str += f"\n• UPC: {upc_code}"
                if package: product_details_str += f"\n• Package quantity: {package}"
                if dimensions: product_details_str += f"\n• Dimensions: {dimensions}"

                if product_rankings:
                    product_details_str += "\n\nProduct rankings:"
                    for rank in product_rankings:
                        product_details_str += f"\n#{rank.split(': ')[1]} in {rank.split(': ')[0]}"

                item_card = {
                    "Title": title or "",
                    "Brand": brand or "",
                    "ID": item_id or "",
                    "Category": category or "",
                    "Category ID": category_id or "",
                    "Price": price or "",
                    "Currency": currency or "",
                    "Available": availability or "",
                    "Rating": str(rating) if rating else "",
                    "Total Rating Count": total_rating_count or "",
                    "Recent Activity Message": recent_activity_message or "",
                    "Product Code UPC": upc_code or "",
                    "Brand Path": "; ".join(all_brand_paths) or "",
                    "Category Path": "; ".join(all_category_paths) or "",
                    "Related Products": "; ".join(combined_related_products) or "",
                    "Description": description or "",
                    "Link": link or "",
                    "Images": ", ".join(regular_image_links) or "",
                    "360 Images": ", ".join(_360_image_links) or "",
                    "Product Details": product_details_str.strip()
                }
                items_data.append(item_card)
                logger.info(f"[{i+1}/{len(items_json_batch)}] Обработан товар: {title}")

            except Exception as e:
                logger.error(f"[{i+1}/{len(items_json_batch)}] Ошибка обработки товара ID {item.get('id', 'unknown')}: {e}")
                continue
        else:
            logger.warning(f"[{i+1}/{len(items_json_batch)}] Пропущен пустой элемент или элемент без ID")

    return items_data

def save_to_json_batch(items_data_batch, filename="iherb_data.json"):
    if not items_data_batch:
        logger.warning("Нет данных для сохранения в пакете JSON")
        return

    logger.info(f"Сохранение {len(items_data_batch)} записей в JSON файл: {filename}")

    try:
        if not os.path.exists(filename):
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(items_data_batch, f, ensure_ascii=False, indent=2)
        else:
            # Читаем существующие данные и добавляем новые
            with open(filename, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            
            existing_data.extend(items_data_batch)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Данные успешно добавлены в {filename}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении JSON: {e}")

def save_to_xml_batch(items_data_batch, filename="iherb_data.xml"):
    if not items_data_batch:
        logger.warning("Нет данных для сохранения в пакете XML")
        return

    logger.info(f"Сохранение {len(items_data_batch)} записей в XML файл: {filename}")

    try:
        if os.path.exists(filename):
            # Если файл существует, читаем его и добавляем новые данные
            tree = ET.parse(filename)
            root = tree.getroot()
        else:
            # Если файла нет, создаем новый корневой элемент
            root = ET.Element("products")
            tree = ET.ElementTree(root)

        # Добавляем новые элементы
        for item in items_data_batch:
            product_elem = ET.SubElement(root, "product")
            
            for key, value in item.items():
                field_elem = ET.SubElement(product_elem, key.replace(" ", "_").replace("360", "three_sixty"))
                field_elem.text = str(value) if value is not None else ""

        # Форматируем XML для лучшей читаемости
        xml_str = ET.tostring(root, encoding='utf-8')
        parsed_xml = minidom.parseString(xml_str)
        pretty_xml = parsed_xml.toprettyxml(indent="  ", encoding='utf-8')
        
        with open(filename, 'wb') as f:
            f.write(pretty_xml)
        
        logger.info(f"Данные успешно добавлены в {filename}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении XML: {e}")


def main():
    logger.info("Начало парсинга iHerb...")
    start = time.perf_counter()

    try:
        logger.info("Загрузка списка страниц...")
        links = get_pages()
        logger.info(f"Получено {len(links)} ссылок")

        if not links:
            logger.error("Не удалось получить ссылки")
            return

        links_to_process = links
        logger.info(f"Будет обработано {len(links_to_process)} ссылок")

        total_processed_items = 0
        batch_num = 0
        
        for items_json_batch in get_items_json_threaded_batched(links_to_process, 
                                                              max_workers=45,  
                                                              batch_size=1000): 
            batch_num += 1
            logger.info(f"Обработка батча {batch_num} ({len(items_json_batch)} элементов)...")
            
            items_data_batch = get_data(items_json_batch)
            
            # Сохраняем результаты
            save_to_json_batch(items_data_batch)
            save_to_xml_batch(items_data_batch)
            
            total_processed_items += len(items_data_batch)
            
            # Прогресс каждые 20%
            progress = total_processed_items / len(links_to_process) * 100
            if progress % 20 <= 1:  # Примерно каждые 20%
                logger.info(f"Прогресс: {progress:.1f}% ({total_processed_items}/{len(links_to_process)})")

        logger.info(f"Парсинг завершен! Успешно: {total_processed_items}, "
                   f"Неудачно: {failed_counter}")

    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        # Всегда закрываем сессии
        session_manager.close_all()
        gc.collect()

    fin = time.perf_counter() - start
    logger.info(f"Парсинг завершен за {fin:.2f} секунд")

if __name__ == "__main__":
    main()