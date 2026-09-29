from http.server import BaseHTTPRequestHandler

BODY = """User-agent: *
Allow: /
Disallow: /api/
Sitemap: https://qiaolian-jinbian-sale.vercel.app/sitemap.xml
"""


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = BODY.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Cache-Control", "public, s-maxage=3600")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
