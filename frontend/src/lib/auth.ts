/**
 * Authentication Helpers for FFP Data Validator
 */

const STORAGE_KEY = "ffp_auth_token";
const USER_KEY = "ffp_user";

export const getBackendUrl = () => {
  // In development, the proxy is handled by Nginx at localhost:3000/api
  // In production, it's the same origin
  return "/api";
};

export const getToken = () => {
  if (typeof window !== "undefined") {
    return localStorage.getItem(STORAGE_KEY);
  }
  return null;
};

export const setToken = (token: string) => {
  if (typeof window !== "undefined") {
    localStorage.setItem(STORAGE_KEY, token);
  }
};

export const clearToken = () => {
  if (typeof window !== "undefined") {
    localStorage.removeItem(STORAGE_KEY);
    localStorage.removeItem(USER_KEY);
  }
};

export const getUser = () => {
  if (typeof window !== "undefined") {
    const user = localStorage.getItem(USER_KEY);
    return user ? JSON.parse(user) : null;
  }
  return null;
};

export const setUser = (user: any) => {
  if (typeof window !== "undefined") {
    localStorage.setItem(USER_KEY, JSON.stringify(user));
  }
};

export const isAdmin = () => {
  const user = getUser();
  return user?.role === "admin";
};

export const isAuthenticated = () => {
  const token = getToken();
  if (!token) return false;
  
  try {
    // Basic JWT check for expiry
    const payload = JSON.parse(atob(token.split(".")[1]));
    const now = Math.floor(Date.now() / 1000);
    return payload.exp > now;
  } catch (e) {
    return false;
  }
};

export const fetchWithAuth = async (url: string, options: RequestInit = {}) => {
  const token = getToken();
  const headers = {
    ...options.headers,
    ...(token ? { "Authorization": `Bearer ${token}` } : {}),
  } as any;

  const response = await fetch(url, { ...options, headers });
  
  if (response.status === 401) {
    clearToken();
    if (typeof window !== "undefined" && !window.location.pathname.startsWith("/login")) {
      window.location.href = "/login";
    }
  }
  
  // Security lockout: show the specific message from the backend
  if (response.status === 503) {
    try {
      const body = await response.clone().json();
      if (body.detail && body.detail.includes("lockout")) {
        alert("⚠️ Security Lockout: " + body.detail);
      }
    } catch { /* ignore parse errors */ }
  }
  
  return response;
};

/**
 * Authenticated Download Helper
 * Uses fetch with auth header and creates a Blob for download
 */
export const downloadFileWithAuth = async (url: string, filename?: string) => {
  try {
    const response = await fetchWithAuth(url);
    if (!response.ok) {
      let errorMsg = `Download failed (HTTP ${response.status})`;
      try {
        const body = await response.json();
        if (body.detail) errorMsg = body.detail;
      } catch { /* ignore */ }
      throw new Error(errorMsg);
    }
    
    // Extract filename from URL if not provided explicitly, then decode URL entities and `+` spaces
    let finalFileName = filename;
    if (!finalFileName) {
      finalFileName = url.split('/').pop() || "download";
    }
    finalFileName = decodeURIComponent(finalFileName.replace(/\+/g, ' '));
    
    const blob = await response.blob();
    const downloadUrl = window.URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = downloadUrl;
    link.download = finalFileName;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(downloadUrl);
  } catch (error) {
    console.error("Authenticated download error:", error);
    alert("Failed to download file. Please check your connection and login status.");
  }
};
