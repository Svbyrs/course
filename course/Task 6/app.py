
from flask import Flask, render_template_string, request
import psycopg2, psycopg2.extras

app = Flask(__name__)
DB = "dbname=course_db user=postgres password=1128327 host=localhost"

HTML = """
<form method="post">
  Locale:
  <select name="locale">
    <option value="en_US" {% if locale=='en_US' %}selected{% endif %}>English</option>
    <option value="de_DE" {% if locale=='de_DE' %}selected{% endif %}>German</option>
  </select>
  Seed: <input type="number" name="seed" value="{{seed}}">
  Batch: <input type="number" name="batch_index" value="{{batch_index}}">
  <button name="action" value="generate">Generate</button>
  <button name="action" value="next">Next Batch</button>
</form>
{% if users %}
<table border="1">
<tr><th>Name</th><th>Email</th><th>Phone</th></tr>
{% for u in users %}
<tr><td>{{u.full_name}}</td><td>{{u.email}}</td><td>{{u.phone}}</td></tr>
{% endfor %}
</table>
{% endif %}
"""

def get_users(locale, seed, batch_index):
    conn = psycopg2.connect(DB)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * FROM generate_fake_users(%s,%s,%s,%s)", (locale, seed, batch_index, 10))
    rows = cur.fetchall()
    conn.close()
    return rows

@app.route("/", methods=["GET","POST"])
def index():
    locale = request.form.get("locale","en_US")
    seed = int(request.form.get("seed","42"))
    batch_index = int(request.form.get("batch_index","0"))
    if request.form.get("action")=="next":
        batch_index += 1
    users = get_users(locale, seed, batch_index) if request.method=="POST" else None
    return render_template_string(HTML, users=users, locale=locale, seed=seed, batch_index=batch_index)


if __name__ == "__main__":
    app.run()

