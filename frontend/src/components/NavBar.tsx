"use client";

import { useEffect, useState } from "react";
import { useRouter, usePathname } from "next/navigation";
import { isAuthenticated, getUser, clearToken, isAdmin } from "@/lib/auth";
import Link from "next/link";

export default function NavBar() {
  const [user, setLocalUser] = useState<any>(null);
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    if (!isAuthenticated() && pathname !== "/login") {
      router.push("/login");
    } else {
      setLocalUser(getUser());
    }
  }, [pathname, router]);

  const handleLogout = () => {
    clearToken();
    router.push("/login");
    setLocalUser(null);
  };

  if (pathname === "/login") return null;

  return (
    <nav className="border-b border-[#1e1e20] bg-[#0a0a0b]/80 backdrop-blur-md sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-6 h-16 flex items-center justify-between">
        <div className="flex items-center space-x-8">
          <Link href="/" className="text-white font-black text-xl tracking-tighter">
            FFP<span className="text-emerald-500">.</span>DATA
          </Link>
          <div className="hidden md:flex items-center space-x-6">
            <Link href="/" className={`text-sm font-medium transition-colors ${pathname === '/' ? 'text-white' : 'text-gray-500 hover:text-white'}`}>Upload</Link>
            <Link href="/search" className={`text-sm font-medium transition-colors ${pathname === '/search' ? 'text-white' : 'text-gray-500 hover:text-white'}`}>Search</Link>
            <Link href="/statistics" className={`text-sm font-medium transition-colors ${pathname === '/statistics' ? 'text-white' : 'text-gray-500 hover:text-white'}`}>Statistics</Link>
            {isAdmin() && (
              <Link href="/admin" className={`text-sm font-medium transition-colors ${pathname === '/admin' ? 'text-white' : 'text-gray-500 hover:text-white'}`}>Admin</Link>
            )}
          </div>
        </div>

        <div className="flex items-center space-x-4">
          {user && (
            <div className="flex items-center">
              <span className="text-xs text-gray-500 mr-4 hidden sm:inline">Logged in as <span className="text-white font-semibold">{user.username}</span></span>
              <button 
                onClick={handleLogout}
                className="text-xs font-bold text-gray-400 hover:text-white transition-colors bg-[#1a1a1c] border border-[#2a2a2e] px-4 py-2 rounded-lg"
              >
                Logout
              </button>
            </div>
          )}
        </div>
      </div>
    </nav>
  );
}
