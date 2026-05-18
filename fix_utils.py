import re

with open("aperturedb/Utils.py", "r") as f:
    text = f.read()

text = text.replace(
"""                                table += (
                                    f'<TR><TD BGCOLOR="{
                                        cp_bg}"><FONT COLOR="{cp_fg}">'
                                    f'<B>{prop.strip()}</B></FONT></TD> '
                                    f'<TD BGCOLOR="{
                                        cp_bg}"><FONT COLOR="{cp_fg}">'
                                    f'{matched:,}</FONT></TD> '
                                    f'<TD BGCOLOR="{
                                        cp_bg}"><FONT COLOR="{cp_fg}">'
                                    f'{idx_str}, {typ}</FONT></TD></TR>'
                                )""",
"""                                table += (
                                    '<TR><TD BGCOLOR="{}"><FONT COLOR="{}">'
                                    '<B>{}</B></FONT></TD> '
                                    '<TD BGCOLOR="{}"><FONT COLOR="{}">'
                                    '{}</FONT></TD> '
                                    '<TD BGCOLOR="{}"><FONT COLOR="{}">'
                                    '{}, {}</FONT></TD></TR>'
                                ).format(cp_bg, cp_fg, prop.strip(), cp_bg, cp_fg, f"{matched:,}", cp_bg, cp_fg, idx_str, typ)"""
)

text = text.replace(
"""                        table += (
                            f'<TR><TD BGCOLOR="{c_bg}" COLSPAN="3" '
                            f'PORT="{connection}"><FONT COLOR="{c_fg}">'
                            f'<B>{
                                connection}</B> ({matched:,})</FONT></TD></TR>'
                        )""",
"""                        table += (
                            '<TR><TD BGCOLOR="{}" COLSPAN="3" '
                            'PORT="{}"><FONT COLOR="{}">'
                            '<B>{}</B> ({:,})</FONT></TD></TR>'
                        ).format(c_bg, connection, c_fg, connection, matched)"""
)

with open("aperturedb/Utils.py", "w") as f:
    f.write(text)
