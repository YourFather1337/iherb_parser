import time
import random
import json
import re
import os
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
import cloudscraper
import lxml.html

# === Настройки ===
SITEMAP_URL = "https://www.iherb.com/sitemaps/products-0-www-0.xml"
os.makedirs("results", exist_ok=True)

# === Вспомогательные функции ===
def get_random_user_agent():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
    ]
    return random.choice(user_agents)


def get_pages():
    """Загружаем sitemap и извлекаем все ссылки товаров"""
    print("📥 Загружаем sitemap iHerb...")
    scraper = cloudscraper.create_scraper()
    try:
        response = scraper.get(SITEMAP_URL, timeout=30)
        response.raise_for_status()
        tree = lxml.html.fromstring(response.content)
        links = tree.xpath("//loc/text()")
        print(f"✅ Найдено {len(links)} ссылок на товары.")
        return links
    except Exception as e:
        print(f"❌ Ошибка при загрузке sitemap: {e}")
        return []


def fetch_item_json(link, scraper, total_links, index):
    """Загружаем JSON-данные для одного товара"""
    item_id = link.strip().split("/")[-1]
    if not item_id:
        return None

    product_url = f"https://catalog.app.iherb.com/product/{item_id}"
    recommendations_url = (
        f"https://catalog.app.iherb.com/recommendations/freqpurchasedtogether?productId={item_id}&pageSize=2&page=1"
    )
    ugc_url = f"https://www.iherb.com/ugc/api/product/{item_id}"

    try:
        headers = {"User-Agent": get_random_user_agent()}
        product_data = scraper.get(product_url, headers=headers, timeout=20).json()
        rec_data = scraper.get(recommendations_url, headers=headers, timeout=20).json()
        ugc_data = scraper.get(ugc_url, headers=headers, timeout=20).json()

        if rec_data:
            product_data["frequently_purchased_together"] = rec_data
        if ugc_data and ugc_data.get("upcCode"):
            product_data["upcCode"] = ugc_data["upcCode"]

        print(f"✅ [{index+1}/{total_links}] {item_id}")
        time.sleep(random.uniform(0.3, 0.8))
        return product_data
    except Exception:
        return None


def get_items_json_threaded_batched(links, max_workers=40, batch_size=300):
    """Загружаем JSON данных для товаров в потоках"""
    scraper = cloudscraper.create_scraper()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(fetch_item_json, link, scraper, len(links), i)
            for i, link in enumerate(links)
        ]
        batch = []
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                batch.append(result)
            if (i + 1) % batch_size == 0 or (i + 1) == len(links):
                if batch:
                    yield batch
                    batch = []


def parse_item(item):
    """Парсим нужные поля из JSON"""
    try:
        title = item.get("displayName", "")
        brand = item.get("brandName", "")
        link = item.get("url", "")
        image_indices = item.get("imageIndices", [])
        image_indices_360 = item.get("imageIndices360", [])
        
        brand_code = item.get("brandCode", "").lower() if item.get("brandCode") else ""
        category = item.get("rootCategoryName", "")
        category_id = item.get("rootCategoryId", "")
        part_num = item.get("partNumber", "").lower().replace("-", "") if item.get("partNumber") else ""
        
        regular_image_links = []
        if brand_code and part_num:
            for idx in image_indices:
                regular_image_links.append(f"https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/{brand_code}/{part_num}/v/{idx}.jpg")
        
        _360_image_links = []
        if brand_code and part_num:
            for idx in image_indices_360:
                _360_image_links.append(f"https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/{brand_code}/{part_num}/v/{idx}.jpg")

        # If no regular images and primaryImageIndex exists, use it as a regular image
        if not regular_image_links and item.get("primaryImageIndex"):
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

        return {
            "Title": title or "",
            "Brand": brand or "",
            "ID": item_id or "",
            "Category": category or "",
            "Category_ID": category_id or "",
            "Price": price or "",
            "Currency": currency or "",
            "Available": availability or "",
            "Rating": str(rating) if rating else "",
            "Total_Rating_Count": total_rating_count or "",
            "Recent_Activity_Message": recent_activity_message or "",
            "Product_Code_UPC": upc_code or "",
            "Brand_Path": "; ".join(all_brand_paths) or "",
            "Category_Path": "; ".join(all_category_paths) or "",
            "Related_Products": "; ".join(combined_related_products) or "",
            "Description": description or "",
            "Link": link or "",
            "Images": ", ".join(regular_image_links) or "",
            "Images_360": ", ".join(_360_image_links) or "",
            "Product_Details": product_details_str.strip()
        }
    except Exception as e:
        print(f"❌ Ошибка парсинга товара: {e}")
        return None


def create_valid_xml_tag(name):
    """Создает валидное имя для XML тега"""
    # Заменяем все недопустимые символы на подчеркивания
    invalid_chars = r'[^a-zA-Z0-9_\-\.]'
    valid_name = re.sub(invalid_chars, '_', name)
    
    # Убеждаемся, что имя не начинается с цифки или других недопустимых символов
    if valid_name and valid_name[0].isdigit():
        valid_name = 'field_' + valid_name
    
    return valid_name


def save_to_xml(data, filename):
    """Сохраняем данные в XML формате"""
    try:
        root = ET.Element("products")
        
        for item in data:
            product_elem = ET.SubElement(root, "product")
            
            for key, value in item.items():
                # Создаем валидное имя тега
                tag_name = create_valid_xml_tag(key)
                
                field_elem = ET.SubElement(product_elem, tag_name)
                
                # Экранируем специальные XML символы
                if value is not None:
                    # Заменяем все специальные XML символы
                    text_content = str(value)
                    text_content = text_content.replace("&", "&amp;")
                    text_content = text_content.replace("<", "&lt;")
                    text_content = text_content.replace(">", "&gt;")
                    text_content = text_content.replace('"', "&quot;")
                    text_content = text_content.replace("'", "&apos;")
                    field_elem.text = text_content
                else:
                    field_elem.text = ""
        
        # Создаем дерево и сохраняем с форматированием
        tree = ET.ElementTree(root)
        
        # Добавляем отступы для читаемости
        def indent(elem, level=0):
            i = "\n" + level * "  "
            if len(elem):
                if not elem.text or not elem.text.strip():
                    elem.text = i + "  "
                if not elem.tail or not elem.tail.strip():
                    elem.tail = i
                for elem in elem:
                    indent(elem, level + 1)
                if not elem.tail or not elem.tail.strip():
                    elem.tail = i
            else:
                if level and (not elem.tail or not elem.tail.strip()):
                    elem.tail = i
        
        indent(root)
        
        # Сохраняем с XML декларацией
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            tree.write(f, encoding='unicode')
        
        print(f"💾 XML данные сохранены в {filename}")
        return True
    except Exception as e:
        print(f"❌ Ошибка при сохранении XML: {e}")
        return False


def check_elems(arr) -> list:
    links = get_pages()
    if not links:
        print("❌ Нет ссылок для обработки.")
        return
    
    for batch_num, batch_json in enumerate(get_items_json_threaded_batched(links)):
        print(f"📦 Обрабатываем пакет {batch_num + 1}...")
        for item_json in batch_json:
            parsed = parse_item(item_json)
            if parsed:
                arr.append(parsed)
    
    return arr


def main_iherb():
    start = time.perf_counter()
    print("🚀 Запуск парсинга iHerb...")

    all_items = []

    newarr = check_elems(all_items)

    # Сохраняем в JSON
    json_filename = "results/iherb.json"
    with open(json_filename, "w", encoding="utf-8") as f:
        json.dump(newarr, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON данные сохранены в {json_filename}")

    # Сохраняем в XML
    xml_filename = "results/iherb.xml"
    if save_to_xml(newarr, xml_filename):
        print(f"✅ XML файл успешно создан и должен открываться без ошибок")
    else:
        print(f"❌ Не удалось создать XML файл")

    end = time.perf_counter()
    print(f"\n✅ Парсинг завершён: {len(all_items)} товаров за {end - start:.2f} сек.")
    print(f"💾 Данные сохранены в:")
    print(f"   - {json_filename}")
    print(f"   - {xml_filename}")


if __name__ == "__main__":
    main_iherb()
