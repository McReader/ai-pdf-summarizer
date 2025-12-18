FROM node:20-alpine

WORKDIR /app

# Install dependencies
RUN apk add --no-cache libc6-compat

# Copy package files
COPY package.json package-lock.json* ./
RUN npm ci

# Copy application code
COPY . .

# Expose port
EXPOSE 3000

# Set environment variable
ENV NEXT_TELEMETRY_DISABLED 1

# Run development server
CMD ["npm", "run", "dev"]

