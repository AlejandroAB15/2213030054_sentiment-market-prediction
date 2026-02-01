def get_text(node, xpath):
    result = node.xpath(xpath)
    return result[0].strip() if result else None
