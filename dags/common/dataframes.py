def to_buffer(df):
    from io import StringIO

    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)
    return buffer
