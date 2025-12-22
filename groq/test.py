from dotenv import load_dotenv
import os
from groq import Groq

load_dotenv()
api_key = os.getenv("GROQ_API_KEY")

client = Groq(api_key=api_key)

completion = client.chat.completions.create(
    model="meta-llama/llama-4-scout-17b-16e-instruct",
    messages=[
        {"role": "system", "content": "You are a helpful assistant that summarizes text in a concise way."},
        {"role": "user", "content": "Who is the president of America?"}
    ],
    temperature=0.7,
    max_completion_tokens=1024,
    top_p=1,
    stream=False
)

print(completion.choices[0].message.content)
